defmodule Cortex.AgentProcess do
  @moduledoc """
  Per-agent GenServer managing CC process lifecycle via tmux.

  Responsibilities:
  1. Start CC in tmux session
  2. Monitor CC liveness (async has-session polling)
  3. Detect idle/busy/dead state (async capture-pane + pattern matching)
  4. Inject text via tmux send-keys (only when idle, sanitized)
  5. Read output via Erlang Port on FIFO pipe
  6. Restart CC on death with exponential backoff
  7. Report state changes via PubSub
  8. Scrape context % from pane content
  9. Sanitize all injection input
  10. Enforce authorization on control commands
  """

  use GenServer
  require Logger

  alias Cortex.AgentProcess.Sanitizer
  alias Phoenix.PubSub

  @pubsub Cortex.PubSub

  # ── State detection patterns ───────────────────────────────────────────

  @braille_spinner ~r/[⣾⣽⣻⢿⡿⣟⣯⣷]/u
  @idle_prompt ~r/[❯>]\s*$/u
  @interactive_prompt ~r/Do you want to|Select a model|trust prompt/iu
  @context_pct_regex ~r/(\d+)%/

  # ── Timing defaults ────────────────────────────────────────────────────

  @default_idle_poll_ms 2_000
  @default_liveness_poll_ms 5_000
  @default_stale_threshold_ms 300_000
  @default_max_restart_backoff_ms 60_000

  # ── Storm detection ────────────────────────────────────────────────────

  @storm_threshold 8
  @storm_window_ms 10_000

  # ── Dedup ──────────────────────────────────────────────────────────────

  @dedup_window_ms 30_000

  # ── tmux circuit breaker ───────────────────────────────────────────────

  @max_tmux_failures 3
  @tmux_health_check_ms 30_000

  # ── Output buffer limits ───────────────────────────────────────────────

  @max_output_lines 500
  @max_line_length 4096

  # ── Context warning thresholds ─────────────────────────────────────────

  @context_thresholds [70, 80, 85, 95, 99]

  # ── Authorization matrix ───────────────────────────────────────────────

  @authorized_control %{
    "chris" => [:all],
    "webbie" => [:freeze, :unfreeze, :peek, :prompt, :status],
    "giga" => [:peek, :status],
    "sandy" => [:peek, :status],
    "rogue" => [:peek, :status]
  }

  @authorized_raw ["chris"]

  @control_commands [:freeze, :unfreeze, :kill, :abort, :peek, :raw, :prompt, :restart]

  # ── Public API ─────────────────────────────────────────────────────────

  def start_link(opts) do
    agent_id = Keyword.fetch!(opts, :agent_id)
    GenServer.start_link(__MODULE__, opts, name: via(agent_id))
  end

  def via(agent_id), do: {:via, Registry, {Cortex.AgentProcessRegistry, agent_id}}

  @doc "Inject text into agent's tmux session (display only, no Enter)."
  def inject(agent_id, text) do
    GenServer.call(via(agent_id), {:inject, text})
  end

  @doc "Inject prompt text (with Enter — agent processes this)."
  def inject_prompt(agent_id, text) do
    GenServer.call(via(agent_id), {:inject_prompt, text})
  end

  @doc "Execute a control command (freeze, unfreeze, peek, etc)."
  def control(agent_id, command, from_peer) do
    GenServer.call(via(agent_id), {:control, command, from_peer})
  end

  @doc "Get current AgentProcess state."
  def get_state(agent_id) do
    GenServer.call(via(agent_id), :get_state)
  catch
    :exit, _ -> %{agent_state: :not_running}
  end

  @doc false
  def detect_state_from_pane(pane_content), do: detect_state(pane_content)

  @doc false
  def authorized_for?(peer_id, command), do: authorized?(peer_id, command)

  # ── GenServer init ─────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    agent_id = Keyword.fetch!(opts, :agent_id)

    # Generation tracking for stale message detection
    generation = (:persistent_term.get({:agent_gen, agent_id}, 0)) + 1
    :persistent_term.put({:agent_gen, agent_id}, generation)

    tmux_socket = Keyword.get(opts, :tmux_socket, agent_id)
    tmux_session = Keyword.get(opts, :tmux_session, agent_id)
    fifo_dir = Keyword.get(opts, :fifo_dir, "/home/#{agent_id}/.overwatch")
    fifo_path = Path.join(fifo_dir, "#{agent_id}.pipe")
    idle_poll_ms = Keyword.get(opts, :idle_poll_ms, @default_idle_poll_ms)
    liveness_poll_ms = Keyword.get(opts, :liveness_poll_ms, @default_liveness_poll_ms)

    state = %{
      agent_id: agent_id,
      tmux_socket: tmux_socket,
      tmux_session: tmux_session,
      cc_bin: Keyword.get(opts, :cc_bin, "claude"),
      cc_work_dir: Keyword.get(opts, :cc_work_dir, "/home/#{agent_id}"),
      cc_args: Keyword.get(opts, :cc_args, ["--dangerously-skip-permissions"]),
      fifo_dir: fifo_dir,
      fifo_path: fifo_path,
      fifo_port: nil,

      # State
      agent_state: :starting,
      last_state_change: now(),
      last_output_at: now(),
      last_context_pct: 0,

      # Polling
      idle_poll_ms: idle_poll_ms,
      liveness_poll_ms: liveness_poll_ms,
      polling: false,
      injecting: false,
      pending_injection: nil,

      # Safety controls
      frozen: false,
      injection_times: [],
      recent_injections: [],

      # tmux circuit breaker
      consecutive_tmux_failures: 0,
      tmux_available: true,

      # Output buffer
      output_buffer: [],

      # Generation
      generation: generation,

      # Restart tracking
      restart_count: 0,
      restart_timer: nil,
      max_restart_backoff_ms:
        Keyword.get(opts, :max_restart_backoff_ms, @default_max_restart_backoff_ms),

      # Config
      stale_threshold_ms:
        Keyword.get(opts, :stale_threshold_ms, @default_stale_threshold_ms)
    }

    Logger.info("[agent:#{agent_id}] AgentProcess starting (gen #{generation})")

    # Start FIFO reader
    state = safe_start_fifo(state)

    # Check if tmux session already exists, start CC if not
    send(self(), :check_or_start_cc)

    # Start poll timers with jitter (prevents thundering herd — NEW-4)
    jitter_poll = idle_poll_ms + :rand.uniform(idle_poll_ms)
    jitter_liveness = liveness_poll_ms + :rand.uniform(liveness_poll_ms)
    Process.send_after(self(), :poll_tick, jitter_poll)
    Process.send_after(self(), :liveness_tick, jitter_liveness)

    {:ok, state}
  end

  # ── Injection calls ────────────────────────────────────────────────────

  @impl true
  def handle_call({:inject, _text}, _from, %{frozen: true} = state) do
    {:reply, {:error, :frozen}, state}
  end

  def handle_call({:inject, text}, from, %{polling: true} = state) do
    # Queue the injection, execute after poll completes
    {:noreply, %{state | pending_injection: {from, {:inject, text}}}}
  end

  def handle_call({:inject, text}, _from, state) do
    case check_injection_guards(text, state) do
      {:ok, state} ->
        do_inject_message(text, state)
        {:reply, :ok, %{state | injecting: true}}

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:inject_prompt, _text}, _from, %{frozen: true} = state) do
    {:reply, {:error, :frozen}, state}
  end

  def handle_call({:inject_prompt, text}, _from, state) do
    case check_injection_guards(text, state) do
      {:ok, state} ->
        do_inject_prompt(text, state)
        {:reply, :ok, %{state | injecting: true}}

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  # ── Control commands ───────────────────────────────────────────────────

  def handle_call({:control, command, from_peer}, _from, state)
      when command in @control_commands do
    if authorized?(from_peer, command) do
      Logger.info("[agent:#{state.agent_id}] Control #{command} from #{from_peer}")
      {result, state} = execute_control(command, from_peer, state)
      {:reply, result, state}
    else
      Logger.warning("[agent:#{state.agent_id}] Unauthorized #{command} from #{from_peer}")
      {:reply, {:error, :unauthorized}, state}
    end
  end

  def handle_call({:control, command, from_peer}, _from, state) do
    Logger.warning("[agent:#{state.agent_id}] Unknown control #{command} from #{from_peer}")
    {:reply, {:error, :unknown_command}, state}
  end

  # ── State query ────────────────────────────────────────────────────────

  def handle_call(:get_state, _from, state) do
    info = %{
      agent_id: state.agent_id,
      agent_state: state.agent_state,
      generation: state.generation,
      frozen: state.frozen,
      tmux_available: state.tmux_available,
      last_context_pct: state.last_context_pct,
      output_buffer_size: length(state.output_buffer),
      consecutive_tmux_failures: state.consecutive_tmux_failures,
      last_output_at: state.last_output_at,
      last_state_change: state.last_state_change
    }

    {:reply, info, state}
  end

  # ── Timer / message handlers ───────────────────────────────────────────

  @impl true
  def handle_info(:check_or_start_cc, state) do
    async_tmux(:has_session, "tmux", [
      "-L", state.tmux_socket, "has-session", "-t", state.tmux_session
    ])

    {:noreply, state}
  end

  # Poll tick — capture pane for state detection
  def handle_info(:poll_tick, %{tmux_available: false} = state) do
    # Circuit breaker open — skip polling
    Process.send_after(self(), :poll_tick, state.idle_poll_ms)
    {:noreply, state}
  end

  def handle_info(:poll_tick, %{injecting: true} = state) do
    # Don't poll during injection — retry shortly
    Process.send_after(self(), :poll_tick, 500)
    {:noreply, state}
  end

  def handle_info(:poll_tick, state) do
    async_tmux(:capture_pane, "tmux", [
      "-L", state.tmux_socket,
      "capture-pane", "-t", state.tmux_session,
      "-p", "-J"
    ])

    Process.send_after(self(), :poll_tick, state.idle_poll_ms)
    {:noreply, %{state | polling: true}}
  end

  # Liveness tick — check if tmux session exists
  def handle_info(:liveness_tick, %{tmux_available: false} = state) do
    Process.send_after(self(), :liveness_tick, state.liveness_poll_ms)
    {:noreply, state}
  end

  def handle_info(:liveness_tick, state) do
    async_tmux(:has_session, "tmux", [
      "-L", state.tmux_socket, "has-session", "-t", state.tmux_session
    ])

    Process.send_after(self(), :liveness_tick, state.liveness_poll_ms)
    {:noreply, state}
  end

  # tmux health check (circuit breaker recovery probe)
  def handle_info(:tmux_health_check, state) do
    async_tmux(:has_session, "tmux", [
      "-L", state.tmux_socket, "has-session", "-t", state.tmux_session
    ])

    Process.send_after(self(), :tmux_health_check, @tmux_health_check_ms)
    {:noreply, state}
  end

  # Delayed Enter after prompt injection
  def handle_info({:send_enter, session}, state) do
    async_tmux(:send_keys, "tmux", [
      "-L", state.tmux_socket, "send-keys", "-t", session, "Enter"
    ])

    {:noreply, %{state | injecting: false}}
  end

  # Attach pipe-pane after CC start (delayed to let tmux initialize)
  def handle_info({:attach_pipe_pane, fifo_path}, state) do
    async_tmux(:pipe_pane, "tmux", [
      "-L", state.tmux_socket,
      "pipe-pane", "-t", state.tmux_session,
      "cat >> #{fifo_path}"
    ])

    {:noreply, state}
  end

  # Restart CC with backoff
  def handle_info(:restart_cc, state) do
    Logger.info("[agent:#{state.agent_id}] Restarting CC (attempt #{state.restart_count})")
    start_cc(state)
    {:noreply, %{state | restart_timer: nil}}
  end

  # FIFO retry
  def handle_info(:retry_fifo, state) do
    state = safe_start_fifo(state)
    {:noreply, state}
  end

  # ── Async task results ─────────────────────────────────────────────────

  def handle_info({ref, {output, exit_code}}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    cmd_type = Process.delete({:tmux_task, ref})
    dispatch_tmux_result(cmd_type, output, exit_code, state)
  end

  def handle_info({:DOWN, ref, :process, _pid, :normal}, state) do
    Process.delete({:tmux_task, ref})
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    cmd_type = Process.delete({:tmux_task, ref})

    if cmd_type do
      handle_tmux_failure(reason, state)
    else
      {:noreply, state}
    end
  end

  # ── FIFO Port messages ─────────────────────────────────────────────────

  def handle_info({port, {:data, data}}, %{fifo_port: port} = state) do
    state = process_output(data, state)
    {:noreply, state}
  end

  def handle_info({port, {:exit_status, _code}}, %{fifo_port: port} = state) do
    Logger.warning("[agent:#{state.agent_id}] FIFO reader died, restarting")

    case safe_start_fifo_reader(state.fifo_path) do
      {:ok, new_port} ->
        {:noreply, %{state | fifo_port: new_port}}

      {:error, _} ->
        Process.send_after(self(), :retry_fifo, 2_000)
        {:noreply, %{state | fifo_port: nil}}
    end
  end

  # Catch-all for unexpected messages
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # ── Terminate ──────────────────────────────────────────────────────────

  @impl true
  def terminate(reason, state) do
    Logger.info("[agent:#{state.agent_id}] AgentProcess terminating: #{inspect(reason)}")

    # Detach pipe-pane (ignore errors)
    try do
      System.cmd("tmux", ["-L", state.tmux_socket, "pipe-pane", "-t", state.tmux_session],
        stderr_to_stdout: true
      )
    rescue
      _ -> :ok
    end

    # Close FIFO port
    if state.fifo_port do
      try do
        Port.close(state.fifo_port)
      rescue
        _ -> :ok
      end
    end

    # Delete FIFO file
    File.rm(state.fifo_path)

    # Do NOT kill tmux session — CC should survive overwatch restart
    :ok
  end

  def child_spec(opts) do
    agent_id = Keyword.fetch!(opts, :agent_id)

    %{
      id: {__MODULE__, agent_id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: :infinity
    }
  end

  # ── Internal: tmux result dispatch ─────────────────────────────────────

  # has-session success — tmux session exists
  defp dispatch_tmux_result(:has_session, _output, 0, state) do
    state = handle_tmux_success(state)

    state =
      if state.agent_state in [:dead, :starting] do
        # Session is live — reset restart counter
        state = %{state | restart_count: 0}
        cancel_restart_timer(state) |> transition_to(:unknown)
      else
        state
      end

    {:noreply, state}
  end

  # has-session failure — tmux session doesn't exist
  defp dispatch_tmux_result(:has_session, _output, _exit_code, state) do
    state = handle_tmux_success(state)

    state =
      if state.agent_state != :dead do
        transition_to(:dead, state)
      else
        state
      end

    state = maybe_schedule_restart(state)
    {:noreply, state}
  end

  # capture-pane success — detect state from pane content
  defp dispatch_tmux_result(:capture_pane, output, 0, state) do
    state = handle_tmux_success(state)
    new_state = detect_state(output)
    state = maybe_transition(new_state, state)
    state = scrape_context_pct(output, state)
    state = handle_pending_injection(%{state | polling: false})
    {:noreply, state}
  end

  # capture-pane failure
  defp dispatch_tmux_result(:capture_pane, _output, _exit_code, state) do
    state = handle_pending_injection(%{state | polling: false})
    {:noreply, state}
  end

  # send-keys success
  defp dispatch_tmux_result(:send_keys, _output, 0, state) do
    {:noreply, state}
  end

  # send-keys failure
  defp dispatch_tmux_result(:send_keys, _output, _exit_code, state) do
    Logger.warning("[agent:#{state.agent_id}] send-keys failed")
    {:noreply, %{state | injecting: false}}
  end

  # new-session success — CC started
  defp dispatch_tmux_result(:new_session, _output, 0, state) do
    Logger.info("[agent:#{state.agent_id}] CC started in tmux")
    state = transition_to(:starting, state)
    {:noreply, state}
  end

  # new-session failure
  defp dispatch_tmux_result(:new_session, output, _exit_code, state) do
    Logger.error("[agent:#{state.agent_id}] Failed to start CC: #{String.trim(output)}")
    state = maybe_schedule_restart(state)
    {:noreply, state}
  end

  # kill-session result
  defp dispatch_tmux_result(:kill_session, _output, _exit_code, state) do
    state = transition_to(:dead, state)
    {:noreply, state}
  end

  # pipe-pane — just log on failure
  defp dispatch_tmux_result(:pipe_pane, _output, 0, state), do: {:noreply, state}

  defp dispatch_tmux_result(:pipe_pane, output, _exit_code, state) do
    Logger.warning("[agent:#{state.agent_id}] pipe-pane failed: #{String.trim(output)}")
    {:noreply, state}
  end

  # Unknown/stale task ref
  defp dispatch_tmux_result(_type, _output, _exit_code, state) do
    {:noreply, state}
  end

  # ── Internal: tmux failure/success tracking ────────────────────────────

  defp handle_tmux_failure(reason, state) do
    failures = state.consecutive_tmux_failures + 1
    Logger.error("[agent:#{state.agent_id}] tmux failure ##{failures}: #{inspect(reason)}")

    if failures >= @max_tmux_failures do
      Logger.error("[agent:#{state.agent_id}] tmux circuit breaker OPEN")
      state = transition_to(:dead, state)
      Process.send_after(self(), :tmux_health_check, @tmux_health_check_ms)

      {:noreply,
       %{state | consecutive_tmux_failures: failures, tmux_available: false, polling: false}}
    else
      {:noreply, %{state | consecutive_tmux_failures: failures, polling: false}}
    end
  end

  defp handle_tmux_success(state) do
    %{state | consecutive_tmux_failures: 0, tmux_available: true}
  end

  # ── Internal: state detection ──────────────────────────────────────────

  defp detect_state(pane_content) when is_binary(pane_content) do
    cond do
      # Braille spinner = CC actively processing
      Regex.match?(@braille_spinner, pane_content) -> :busy

      # CC idle prompt (check AFTER spinner — prompt can coexist briefly)
      Regex.match?(@idle_prompt, pane_content) -> :idle

      # Interactive prompts = busy (needs intervention)
      Regex.match?(@interactive_prompt, pane_content) -> :busy

      # No signal — conservative unknown (M1 fix)
      true -> :unknown
    end
  end

  defp maybe_transition(new_state, %{agent_state: current} = state) when new_state == current do
    # Check for stale transition: unknown + no output for stale_threshold
    if new_state == :unknown and now() - state.last_output_at > state.stale_threshold_ms do
      transition_to(:stale, state)
    else
      state
    end
  end

  defp maybe_transition(new_state, state) do
    transition_to(new_state, state)
  end

  defp transition_to(new_state, %{agent_state: current} = state) when new_state == current do
    state
  end

  defp transition_to(new_state, state) do
    old = state.agent_state
    Logger.info("[agent:#{state.agent_id}] #{old} -> #{new_state}")
    PubSub.broadcast(@pubsub, "agent:#{state.agent_id}", {:state_changed, new_state})

    state = %{state | agent_state: new_state, last_state_change: now()}

    # Reset restart count on successful CC start
    if new_state in [:idle, :busy] and old in [:starting, :dead, :unknown] do
      %{state | restart_count: 0}
    else
      state
    end
  end

  # ── Internal: context % scraping ───────────────────────────────────────

  defp scrape_context_pct(pane_content, state) do
    case Regex.scan(@context_pct_regex, pane_content) do
      [] ->
        state

      matches ->
        # Take the last match (most recent line)
        [_, pct_str] = List.last(matches)
        pct = String.to_integer(pct_str)

        if pct > 0 and pct <= 100 do
          state = check_context_thresholds(pct, state)
          %{state | last_context_pct: pct}
        else
          state
        end
    end
  end

  defp check_context_thresholds(pct, state) do
    newly_crossed =
      Enum.filter(@context_thresholds, fn t ->
        pct >= t and state.last_context_pct < t
      end)

    Enum.each(newly_crossed, fn threshold ->
      PubSub.broadcast(
        @pubsub,
        "agent:#{state.agent_id}",
        {:context_warning, threshold, pct}
      )

      Logger.warning(
        "[agent:#{state.agent_id}] Context #{pct}% — crossed #{threshold}% threshold"
      )
    end)

    state
  end

  # ── Internal: start CC ─────────────────────────────────────────────────

  defp start_cc(state) do
    cc_cmd = Enum.join([state.cc_bin | state.cc_args], " ")
    tmux_cmd = "cd #{state.cc_work_dir} && #{cc_cmd}"

    async_tmux(:new_session, "tmux", [
      "-L", state.tmux_socket,
      "new-session", "-d",
      "-s", state.tmux_session,
      "bash", "-l", "-c", tmux_cmd
    ])

    # Attach pipe-pane for FIFO output (delayed to let tmux initialize)
    if state.fifo_path do
      Process.send_after(self(), {:attach_pipe_pane, state.fifo_path}, 1_000)
    end
  end

  defp maybe_schedule_restart(%{restart_timer: ref} = state) when is_reference(ref) do
    # Already scheduled
    state
  end

  defp maybe_schedule_restart(state) do
    backoff =
      min(
        1000 * round(:math.pow(2, state.restart_count)),
        state.max_restart_backoff_ms
      )

    Logger.info(
      "[agent:#{state.agent_id}] Scheduling CC restart in #{backoff}ms (attempt #{state.restart_count + 1})"
    )

    ref = Process.send_after(self(), :restart_cc, backoff)
    %{state | restart_timer: ref, restart_count: state.restart_count + 1}
  end

  defp cancel_restart_timer(%{restart_timer: nil} = state), do: state

  defp cancel_restart_timer(%{restart_timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | restart_timer: nil}
  end

  # ── Internal: FIFO reader ──────────────────────────────────────────────

  defp safe_start_fifo(state) do
    case safe_start_fifo_reader(state.fifo_path) do
      {:ok, port} ->
        %{state | fifo_port: port}

      {:error, reason} ->
        Logger.warning("[agent:#{state.agent_id}] FIFO start failed: #{inspect(reason)}")
        Process.send_after(self(), :retry_fifo, 2_000)
        %{state | fifo_port: nil}
    end
  end

  defp safe_start_fifo_reader(fifo_path) do
    try do
      fifo_dir = Path.dirname(fifo_path)
      File.mkdir_p!(fifo_dir)
      File.chmod!(fifo_dir, 0o700)

      unless File.exists?(fifo_path) do
        # Create FIFO with restrictive umask (NEW-2 fix)
        {_, 0} = System.cmd("bash", ["-c", "umask 0077 && mkfifo #{fifo_path}"])
      end

      # Verify not a symlink (I4)
      case File.lstat(fifo_path) do
        {:ok, %{type: :other}} ->
          File.chmod!(fifo_path, 0o600)
          port = Port.open({:spawn, "cat #{fifo_path}"}, [:binary, :stream, :exit_status])
          {:ok, port}

        {:ok, stat} ->
          {:error, "FIFO path is not a FIFO (type: #{stat.type}): #{fifo_path}"}

        {:error, reason} ->
          {:error, reason}
      end
    rescue
      e -> {:error, Exception.message(e)}
    end
  end

  defp process_output(data, state) do
    lines = String.split(data, "\n", trim: true)
    lines = Enum.map(lines, &String.slice(&1, 0, @max_line_length))
    buffer = Enum.take(state.output_buffer ++ lines, -@max_output_lines)
    %{state | output_buffer: buffer, last_output_at: now()}
  end

  # ── Internal: injection ────────────────────────────────────────────────

  defp check_injection_guards(text, state) do
    case check_storm(state) do
      {:storm, state} ->
        {:error, :storm, state}

      {:ok, state} ->
        {duplicate, state} = is_duplicate?(text, state)

        if duplicate do
          {:error, :duplicate, state}
        else
          {:ok, state}
        end
    end
  end

  defp do_inject_message(text, state) do
    clean = Sanitizer.sanitize(text)

    if Sanitizer.was_modified?(text, clean) do
      Logger.warning("[agent:#{state.agent_id}] Sanitized injection input")
    end

    # send-keys -l = literal (no key interpretation)
    # NO trailing Enter — this is display, not a command
    async_tmux(:send_keys, "tmux", [
      "-L", state.tmux_socket, "send-keys", "-t", state.tmux_session, "-l", clean
    ])
  end

  defp do_inject_prompt(text, state) do
    clean = Sanitizer.sanitize(text)

    if Sanitizer.was_modified?(text, clean) do
      Logger.warning("[agent:#{state.agent_id}] Sanitized prompt injection input")
    end

    async_tmux(:send_keys, "tmux", [
      "-L", state.tmux_socket, "send-keys", "-t", state.tmux_session, "-l", clean
    ])

    # Small delay before Enter to let tmux process the text
    Process.send_after(self(), {:send_enter, state.tmux_session}, 100)
  end

  defp handle_pending_injection(%{pending_injection: nil} = state), do: state

  defp handle_pending_injection(%{pending_injection: {from, {:inject, text}}} = state) do
    case check_injection_guards(text, state) do
      {:ok, state} ->
        do_inject_message(text, state)
        GenServer.reply(from, :ok)
        %{state | pending_injection: nil, injecting: true}

      {:error, reason, state} ->
        GenServer.reply(from, {:error, reason})
        %{state | pending_injection: nil}
    end
  end

  # ── Internal: storm detection ──────────────────────────────────────────

  defp check_storm(state) do
    now = now()
    window = Enum.filter(state.injection_times, &(&1 > now - @storm_window_ms))

    if length(window) >= @storm_threshold do
      Logger.error(
        "[agent:#{state.agent_id}] STORM DETECTED — #{length(window)} injections in #{@storm_window_ms}ms, freezing"
      )

      PubSub.broadcast(@pubsub, "agent:#{state.agent_id}", {:storm_detected, length(window)})
      {:storm, %{state | injection_times: window, frozen: true}}
    else
      {:ok, %{state | injection_times: [now | window]}}
    end
  end

  # ── Internal: dedup ────────────────────────────────────────────────────

  defp is_duplicate?(text, state) do
    now = now()

    recent =
      Enum.filter(state.recent_injections, fn {_text, ts} ->
        ts > now - @dedup_window_ms
      end)

    duplicate = Enum.any?(recent, fn {prev_text, _ts} -> prev_text == text end)

    if duplicate do
      Logger.debug("[agent:#{state.agent_id}] Dedup: suppressed duplicate injection")
    end

    {duplicate, %{state | recent_injections: [{text, now} | recent]}}
  end

  # ── Internal: control command execution ────────────────────────────────

  defp execute_control(:freeze, _from_peer, state) do
    Logger.info("[agent:#{state.agent_id}] FROZEN")
    {:ok, %{state | frozen: true}}
  end

  defp execute_control(:unfreeze, _from_peer, state) do
    Logger.info("[agent:#{state.agent_id}] UNFROZEN")
    {:ok, %{state | frozen: false}}
  end

  defp execute_control(:peek, _from_peer, state) do
    {{:ok, state.output_buffer}, state}
  end

  defp execute_control(:status, _from_peer, state) do
    info = %{
      agent_state: state.agent_state,
      frozen: state.frozen,
      generation: state.generation,
      last_context_pct: state.last_context_pct,
      tmux_available: state.tmux_available,
      buffer_size: length(state.output_buffer)
    }

    {{:ok, info}, state}
  end

  defp execute_control(:kill, _from_peer, state) do
    Logger.warning("[agent:#{state.agent_id}] KILL command — destroying tmux session")

    async_tmux(:kill_session, "tmux", [
      "-L", state.tmux_socket, "kill-session", "-t", state.tmux_session
    ])

    {:ok, state}
  end

  defp execute_control(:abort, _from_peer, state) do
    async_tmux(:send_keys, "tmux", [
      "-L", state.tmux_socket, "send-keys", "-t", state.tmux_session, "C-c"
    ])

    {:ok, state}
  end

  defp execute_control(:restart, _from_peer, state) do
    Logger.warning("[agent:#{state.agent_id}] RESTART command")

    async_tmux(:kill_session, "tmux", [
      "-L", state.tmux_socket, "kill-session", "-t", state.tmux_session
    ])

    # CC will be restarted by the next liveness check detecting :dead
    {:ok, state}
  end

  defp execute_control(:raw, from_peer, state) do
    if from_peer in @authorized_raw do
      {:ok, state}
    else
      {{:error, :unauthorized_raw}, state}
    end
  end

  defp execute_control(:prompt, _from_peer, state) do
    {:ok, state}
  end

  # ── Internal: authorization ────────────────────────────────────────────

  defp authorized?(peer_id, command) do
    allowed = Map.get(@authorized_control, peer_id, [])
    :all in allowed or command in allowed
  end

  # ── Internal: async tmux ───────────────────────────────────────────────

  defp async_tmux(cmd_type, cmd, args) do
    task =
      Task.Supervisor.async_nolink(Cortex.TaskSupervisor, fn ->
        System.cmd(cmd, args, stderr_to_stdout: true)
      end)

    Process.put({:tmux_task, task.ref}, cmd_type)
    task
  end

  # ── Internal: helpers ──────────────────────────────────────────────────

  defp now, do: System.system_time(:millisecond)
end
