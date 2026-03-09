defmodule Cortex.AgentManager do
  @moduledoc """
  Per-agent companion process on the BEAM. Manages:

  - **Nudger**: Fires when agent has been idle for `idle_timeout_ms`.
    Agent responds with ".\\n" to enter cooldown, anything else = back to busy.
  - **Cooldown**: Agent told us to buzz off. Messages queue silently.
    Exits on: wake_at alarm, @mention, !urgent, or cooldown_ms expiry.
  - **Message gate**: While busy or cooldown, inbound bridge messages queue.
    On idle/nudge, agent gets a summary with msg ID range (e.g. 100-107).
    Agent queries what it wants. @mentions and !urgent always bypass.
  - **Wake-at alarm**: Agent can schedule a specific wakeup time.
    Uses monotonic offset so it doesn't drift. Persisted to survive restarts.
  - **Context prefix**: Every injected line gets timestamp, burn%, reset timer,
    queue depth, online peers.
  - **Busy guard**: No injection while agent is busy. Messages queue.
    Prevents the wall-of-text-per-turn bug.

  ## State machine

      IDLE ──(idle_timeout)──▶ NUDGE ──(agent responds)──▶ BUSY
        ▲                       │                            │
        │                  agent says "."              (spinner stops,
        │                       │                       no output for
        │                       ▼                       stale_threshold)
        │                   COOLDOWN                        │
        │                       │                           ▼
        │                       │                        STALE
        │                       │                   (escalate, never kill)
        │                       │                    only !urgent or
        │                       │                    manual abort → IDLE
        │                       │
        ◀── CC prompt detected ─┘
                (via AgentProcess PubSub or Overwatch)

  ## Message summary format

      [14:32:07 UTC | burn: 42% | reset: 2h18m | queue: 3 | online: giga sandy]
      Bridge: msgs 100-107 (8 new, 2 from giga, 1 @mention held)
      Nudge: idle 5m. What's next?
  """

  use GenServer
  require Logger

  alias Cortex.Registry
  alias Phoenix.PubSub

  @pubsub Cortex.PubSub
  @default_idle_timeout_ms 120_000       # 2 min default idle before nudge
  @default_cooldown_ms     300_000       # 5 min default cooldown
  @max_cooldown_ms         3_600_000     # 1 hour max cooldown
  @max_queue_size          500           # FIX v2#2: cap message queue
  @alarm_persist_dir       "/tmp/cortex/alarms"

  # ── Public API ─────────────────────────────────────────────────────────

  def start_link(opts) do
    agent_id = Keyword.fetch!(opts, :agent_id)
    GenServer.start_link(__MODULE__, opts, name: via(agent_id))
  end

  def via(agent_id), do: {:via, Elixir.Registry, {Cortex.AgentManagerRegistry, agent_id}}

  @doc "Notify that the agent's CC state changed (from external Overwatch via registry)."
  def agent_state_changed(agent_id, new_state) do
    safe_cast(agent_id, {:agent_state, new_state})
  end

  @doc "Inbound bridge message for this agent."
  def bridge_message(agent_id, msg) do
    safe_cast(agent_id, {:bridge_msg, msg})
  end

  @doc "Agent wants to set its idle timeout."
  def set_idle_timeout(agent_id, ms) do
    safe_cast(agent_id, {:set_idle_timeout, ms})
  end

  @doc "Agent wants to set a wake-at time (Unix ms)."
  def set_wake_at(agent_id, unix_ms) do
    safe_cast(agent_id, {:set_wake_at, unix_ms})
  end

  @doc "Agent wants to set cooldown duration."
  def set_cooldown(agent_id, ms) do
    safe_cast(agent_id, {:set_cooldown, ms})
  end

  @doc "Agent responded to a nudge (Overwatch detected output)."
  def agent_responded(agent_id, response) do
    safe_cast(agent_id, {:agent_responded, response})
  end

  @doc "Fetch queued messages by ID range."
  def get_messages(agent_id, from_id, to_id) do
    GenServer.call(via(agent_id), {:get_messages, from_id, to_id})
  end

  @doc "Get current nudger state for dashboard."
  def get_state(agent_id) do
    GenServer.call(via(agent_id), :get_state)
  catch
    :exit, _ -> %{phase: :not_running}
  end

  # ── GenServer init ─────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    agent_id = Keyword.fetch!(opts, :agent_id)

    # Subscribe to agent PubSub (receives :state_changed from AgentProcess)
    PubSub.subscribe(@pubsub, "agent:#{agent_id}")

    state = %{
      agent_id: agent_id,

      # Phase: :idle | :busy | :stale | :nudge_pending | :cooldown
      phase: :idle,

      # Timers
      idle_timeout_ms: @default_idle_timeout_ms,
      cooldown_ms: @default_cooldown_ms,
      idle_timer: nil,
      cooldown_timer: nil,
      wake_at_timer: nil,
      wake_at_unix: nil,

      # Message queue (bridge messages held while busy/cooldown)
      msg_queue: [],           # [{seq, msg}]
      msg_seq: 0,              # monotonic sequence counter
      last_delivered_seq: 0,   # last seq included in a summary

      # Urgent/mention queue (always delivered immediately)
      urgent_queue: [],

      # Tracking
      idle_since: System.system_time(:millisecond),
      busy_since: nil,
      last_nudge: nil,
      last_activity: System.system_time(:millisecond),

      # Agent-reported context
      burn_pct: 0,
      reset_in_ms: 0,
      token_info: %{},
    }

    # Check for persisted wake-at alarm
    state = restore_alarm(state)

    # Start idle timer
    state = start_idle_timer(state)

    # Query AgentProcess for current state (NEW-5 fix: handle PubSub race)
    send(self(), :sync_agent_state)

    Logger.info("[nudger:#{agent_id}] Started — idle timeout #{state.idle_timeout_ms}ms")
    {:ok, state}
  end

  # ── PubSub state changes from AgentProcess (atom-based) ────────────────

  @impl true
  def handle_info({:state_changed, :busy}, state) do
    state = cancel_idle_timer(state)
    state = %{state | phase: :busy, busy_since: now()}
    Logger.debug("[nudger:#{state.agent_id}] -> busy (via AgentProcess)")
    {:noreply, state}
  end

  def handle_info({:state_changed, :stale}, state) do
    state = cancel_idle_timer(state)
    state = %{state | phase: :stale}
    Logger.warning("[nudger:#{state.agent_id}] -> stale (via AgentProcess)")
    {:noreply, state}
  end

  def handle_info({:state_changed, :idle}, %{phase: phase} = state)
      when phase in [:busy, :stale] do
    state = %{state | phase: :idle, idle_since: now(), busy_since: nil}
    state = maybe_deliver_summary(state)
    state = start_idle_timer(state)
    Logger.debug("[nudger:#{state.agent_id}] -> idle (from #{phase}, via AgentProcess)")
    {:noreply, state}
  end

  def handle_info({:state_changed, :idle}, state) do
    # Already idle or coming from cooldown — reset timer
    state = %{state | phase: :idle, idle_since: now()}
    state = start_idle_timer(state)
    {:noreply, state}
  end

  def handle_info({:state_changed, :dead}, state) do
    state = cancel_idle_timer(state)
    Logger.warning("[nudger:#{state.agent_id}] -> dead (via AgentProcess)")
    {:noreply, %{state | phase: :stale}}
  end

  def handle_info({:state_changed, _other}, state) do
    {:noreply, state}
  end

  # Context warnings from AgentProcess
  def handle_info({:context_warning, threshold, pct}, state) do
    Logger.warning("[nudger:#{state.agent_id}] Context #{pct}% (threshold #{threshold}%)")
    state = %{state | burn_pct: pct}
    {:noreply, state}
  end

  # Storm detected by AgentProcess
  def handle_info({:storm_detected, count}, state) do
    Logger.error("[nudger:#{state.agent_id}] Storm detected: #{count} injections")
    {:noreply, state}
  end

  # Sync state with AgentProcess on startup (NEW-5 fix)
  def handle_info(:sync_agent_state, state) do
    case Cortex.AgentProcess.get_state(state.agent_id) do
      %{agent_state: :not_running} -> :ok
      %{agent_state: agent_state} ->
        send(self(), {:state_changed, agent_state})
      _ -> :ok
    end
    {:noreply, state}
  end

  # PubSub peer events (ignore — we handle state_changed directly)
  def handle_info({event, _data}, state)
      when event in [:peer_connected, :peer_disconnected, :peer_updated] do
    {:noreply, state}
  end

  # ── Timer fires ────────────────────────────────────────────────────────

  def handle_info(:idle_timeout, %{phase: :idle} = state) do
    Logger.info("[nudger:#{state.agent_id}] Idle timeout — nudging")
    state = %{state | phase: :nudge_pending, last_nudge: now()}
    state = deliver_nudge(state)
    {:noreply, state}
  end

  def handle_info(:idle_timeout, state) do
    {:noreply, state}
  end

  def handle_info(:cooldown_expired, %{phase: :cooldown} = state) do
    Logger.info("[nudger:#{state.agent_id}] Cooldown expired — back to idle")
    state = %{state | phase: :idle, idle_since: now()}
    state = maybe_deliver_summary(state)
    state = start_idle_timer(state)
    {:noreply, state}
  end

  def handle_info(:cooldown_expired, state), do: {:noreply, state}

  def handle_info(:wake_at_alarm, state) do
    Logger.info("[nudger:#{state.agent_id}] Wake-at alarm fired!")
    clear_persisted_alarm(state)

    state = %{state | wake_at_timer: nil, wake_at_unix: nil}

    case state.phase do
      :cooldown ->
        state = %{state | phase: :idle, idle_since: now()}
        state = cancel_cooldown_timer(state)
        state = maybe_deliver_summary(state)
        state = start_idle_timer(state)
        {:noreply, state}

      :idle ->
        state = deliver_nudge(state)
        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  # Catch-all for unexpected messages
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # ── Legacy agent state transitions (from external Overwatch, string-based) ──

  @impl true
  def handle_cast({:agent_state, "busy"}, state) do
    state = cancel_idle_timer(state)
    state = %{state | phase: :busy, busy_since: now()}
    Logger.debug("[nudger:#{state.agent_id}] -> busy")
    {:noreply, state}
  end

  def handle_cast({:agent_state, "stale"}, state) do
    state = cancel_idle_timer(state)
    state = %{state | phase: :stale}
    Logger.warning("[nudger:#{state.agent_id}] -> stale (wedged?)")
    {:noreply, state}
  end

  def handle_cast({:agent_state, "idle"}, %{phase: :busy} = state) do
    state = %{state | phase: :idle, idle_since: now(), busy_since: nil}
    state = maybe_deliver_summary(state)
    state = start_idle_timer(state)
    Logger.debug("[nudger:#{state.agent_id}] -> idle (from busy)")
    {:noreply, state}
  end

  def handle_cast({:agent_state, "idle"}, %{phase: :stale} = state) do
    Logger.info("[nudger:#{state.agent_id}] -> idle (recovered from stale)")
    state = %{state | phase: :idle, idle_since: now(), busy_since: nil}
    state = maybe_deliver_summary(state)
    state = start_idle_timer(state)
    {:noreply, state}
  end

  def handle_cast({:agent_state, "idle"}, state) do
    state = %{state | phase: :idle, idle_since: now()}
    state = start_idle_timer(state)
    {:noreply, state}
  end

  def handle_cast({:agent_state, _other}, state) do
    {:noreply, state}
  end

  # ── Bridge message inbound ─────────────────────────────────────────────

  def handle_cast({:bridge_msg, msg}, state) do
    seq = state.msg_seq + 1
    is_mention = is_mention?(msg, state.agent_id)
    is_urgent = is_urgent?(msg)

    # FIX #11: Prune old delivered messages to prevent unbounded growth
    pruned_queue =
      if length(state.msg_queue) >= @max_queue_size do
        Enum.filter(state.msg_queue, fn {s, _} -> s > state.last_delivered_seq end)
        |> Enum.take(-@max_queue_size)
      else
        state.msg_queue
      end

    cond do
      # @mention or !urgent — always deliver immediately
      is_urgent ->
        state = inject_urgent(state, msg, seq, :urgent)
        {:noreply, %{state | msg_seq: seq, last_activity: now()}}

      is_mention ->
        state = inject_urgent(state, msg, seq, :mention)
        {:noreply, %{state | msg_seq: seq, last_activity: now()}}

      # Agent is idle — queue and reset idle timer (activity resets nudger)
      state.phase == :idle ->
        state = %{state | msg_queue: pruned_queue ++ [{seq, msg}], msg_seq: seq, last_activity: now()}
        state = start_idle_timer(state)
        {:noreply, state}

      # Busy or cooldown — queue silently
      true ->
        state = %{state | msg_queue: pruned_queue ++ [{seq, msg}], msg_seq: seq}
        {:noreply, state}
    end
  end

  # ── Config changes from agent ──────────────────────────────────────────

  def handle_cast({:set_idle_timeout, ms}, state) do
    ms = clamp(ms, 5_000, @max_cooldown_ms)
    Logger.info("[nudger:#{state.agent_id}] Idle timeout -> #{ms}ms")
    state = %{state | idle_timeout_ms: ms}
    state = if state.phase == :idle, do: start_idle_timer(state), else: state
    {:noreply, state}
  end

  def handle_cast({:set_cooldown, ms}, state) do
    ms = clamp(ms, 10_000, @max_cooldown_ms)
    Logger.info("[nudger:#{state.agent_id}] Cooldown -> #{ms}ms")
    {:noreply, %{state | cooldown_ms: ms}}
  end

  def handle_cast({:set_wake_at, unix_ms}, state) do
    state = schedule_wake_at(state, unix_ms)
    persist_alarm(state)
    Logger.info("[nudger:#{state.agent_id}] Wake-at -> #{format_time(unix_ms)}")
    {:noreply, state}
  end

  # ── Agent response detection ───────────────────────────────────────────

  def handle_cast({:agent_responded, response}, %{phase: :nudge_pending} = state) do
    trimmed = String.trim(response)

    if trimmed == "." do
      Logger.info("[nudger:#{state.agent_id}] Agent declined nudge -> cooldown #{state.cooldown_ms}ms")
      state = %{state | phase: :cooldown}
      state = start_cooldown_timer(state)
      {:noreply, state}
    else
      state = %{state | phase: :busy, busy_since: now()}
      {:noreply, state}
    end
  end

  def handle_cast({:agent_responded, _}, state), do: {:noreply, state}

  # ── Queries ────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:get_messages, from_id, to_id}, _from, state) do
    msgs =
      state.msg_queue
      |> Enum.filter(fn {seq, _} -> seq >= from_id and seq <= to_id end)
      |> Enum.map(fn {seq, msg} -> %{seq: seq, msg: msg} end)

    {:reply, msgs, state}
  end

  def handle_call(:get_state, _from, state) do
    info = %{
      agent_id: state.agent_id,
      phase: state.phase,
      idle_timeout_ms: state.idle_timeout_ms,
      cooldown_ms: state.cooldown_ms,
      queued_msgs: length(state.msg_queue),
      msg_seq: state.msg_seq,
      last_delivered_seq: state.last_delivered_seq,
      idle_since: state.idle_since,
      busy_since: state.busy_since,
      wake_at_unix: state.wake_at_unix,
      burn_pct: state.burn_pct,
    }

    {:reply, info, state}
  end

  # ── Internal: nudge delivery ───────────────────────────────────────────

  defp deliver_nudge(state) do
    context = build_context_line(state)
    summary = build_message_summary(state)
    idle_duration = format_duration(now() - state.idle_since)

    lines =
      [context] ++
      (if summary != "", do: [summary], else: []) ++
      ["Nudge: idle #{idle_duration}. What's next?"]

    text = Enum.join(lines, "\n")
    inject_to_agent(state, text)

    state
  end

  defp maybe_deliver_summary(state) do
    pending = state.msg_queue |> Enum.filter(fn {seq, _} -> seq > state.last_delivered_seq end)

    if pending != [] do
      context = build_context_line(state)
      summary = build_message_summary(state)

      if summary != "" do
        inject_to_agent(state, context <> "\n" <> summary)
      end

      %{state | last_delivered_seq: state.msg_seq}
    else
      state
    end
  end

  # ── Internal: message summary builder ──────────────────────────────────

  defp build_message_summary(state) do
    pending = state.msg_queue |> Enum.filter(fn {seq, _} -> seq > state.last_delivered_seq end)

    if pending == [] do
      ""
    else
      seqs = Enum.map(pending, fn {seq, _} -> seq end)
      min_seq = Enum.min(seqs)
      max_seq = Enum.max(seqs)
      count = length(pending)

      by_sender =
        pending
        |> Enum.map(fn {_, msg} -> extract_sender(msg) end)
        |> Enum.frequencies()
        |> Enum.map(fn {sender, n} -> "#{n} from #{sender}" end)
        |> Enum.join(", ")

      mention_count =
        pending
        |> Enum.count(fn {_, msg} -> is_mention?(msg, state.agent_id) end)

      mention_note = if mention_count > 0, do: ", #{mention_count} @mention", else: ""

      "Bridge: msgs #{min_seq}-#{max_seq} (#{count} new: #{by_sender}#{mention_note})"
    end
  end

  # ── Internal: context line builder ─────────────────────────────────────

  defp build_context_line(state) do
    time = Calendar.strftime(DateTime.utc_now(), "%H:%M:%S UTC")
    burn = "burn: #{state.burn_pct}%"

    reset =
      if state.reset_in_ms > 0,
        do: "reset: #{format_duration(state.reset_in_ms)}",
        else: "reset: ?"

    queue_count = Enum.count(state.msg_queue, fn {seq, _} -> seq > state.last_delivered_seq end)
    queue = "queue: #{queue_count}"

    online =
      Registry.list_all()
      |> Enum.filter(&(&1.peer_kind == :agent and &1.peer_id != state.agent_id))
      |> Enum.map(& &1.peer_id)
      |> Enum.join(" ")

    online_str = if online == "", do: "solo", else: online

    "[#{time} | #{burn} | #{reset} | #{queue} | online: #{online_str}]"
  end

  # ── Internal: injection ────────────────────────────────────────────────

  defp inject_to_agent(state, text) do
    # Try AgentProcess first (managed agent), fallback to legacy socket
    case GenServer.whereis(Cortex.AgentProcess.via(state.agent_id)) do
      nil ->
        # Fallback: send to the agent's Overwatch via Cortex routing
        case Registry.find(state.agent_id) do
          {:ok, peer} ->
            send(peer.pid, {:cortex_message, %{
              type: "nudge_inject",
              from: "cortex",
              text: text,
              ts: now(),
            }})

          :not_found ->
            Logger.warning("[nudger:#{state.agent_id}] Agent not connected — can't inject")
        end

      _pid ->
        case Cortex.AgentProcess.inject_prompt(state.agent_id, text) do
          :ok -> :ok
          {:error, reason} ->
            Logger.warning("[nudger:#{state.agent_id}] Injection failed: #{inspect(reason)}")
        end
    end
  end

  defp inject_urgent(state, msg, seq, reason) do
    context = build_context_line(state)
    sender = extract_sender(msg)
    content = extract_content(msg)
    prefix = if reason == :urgent, do: "!URGENT", else: "@MENTION"

    text = "#{context}\n#{prefix} from #{sender} (msg #{seq}): #{truncate(content, 200)}"

    state = %{state |
      msg_queue: state.msg_queue ++ [{seq, msg}],
      last_delivered_seq: max(state.last_delivered_seq, seq)
    }

    state =
      if state.phase == :cooldown do
        cancel_cooldown_timer(state)
        |> Map.put(:phase, :idle)
        |> Map.put(:idle_since, now())
      else
        state
      end

    inject_to_agent(state, text)
    state
  end

  # ── Internal: timers ───────────────────────────────────────────────────

  defp start_idle_timer(state) do
    state = cancel_idle_timer(state)
    ref = Process.send_after(self(), :idle_timeout, state.idle_timeout_ms)
    %{state | idle_timer: ref}
  end

  defp cancel_idle_timer(%{idle_timer: nil} = state), do: state
  defp cancel_idle_timer(%{idle_timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | idle_timer: nil}
  end

  defp start_cooldown_timer(state) do
    state = cancel_cooldown_timer(state)
    ref = Process.send_after(self(), :cooldown_expired, state.cooldown_ms)
    %{state | cooldown_timer: ref}
  end

  defp cancel_cooldown_timer(%{cooldown_timer: nil} = state), do: state
  defp cancel_cooldown_timer(%{cooldown_timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | cooldown_timer: nil}
  end

  defp schedule_wake_at(state, unix_ms) do
    if state.wake_at_timer, do: Process.cancel_timer(state.wake_at_timer)
    delay_ms = max(unix_ms - wall_now(), 0)
    ref = Process.send_after(self(), :wake_at_alarm, delay_ms)
    %{state | wake_at_timer: ref, wake_at_unix: unix_ms}
  end

  # ── Internal: alarm persistence ────────────────────────────────────────

  defp persist_alarm(state) do
    if state.wake_at_unix do
      File.mkdir_p!(@alarm_persist_dir)
      safe_id = sanitize_filename(state.agent_id)
      path = Path.join(@alarm_persist_dir, "#{safe_id}.alarm")
      File.write!(path, "#{state.wake_at_unix}")
    end
  end

  defp restore_alarm(state) do
    safe_id = sanitize_filename(state.agent_id)
    path = Path.join(@alarm_persist_dir, "#{safe_id}.alarm")

    case File.read(path) do
      {:ok, raw} ->
        try do
          unix_ms = String.trim(raw) |> String.to_integer()

          if unix_ms > wall_now() do
            Logger.info("[nudger:#{state.agent_id}] Restoring alarm at #{format_time(unix_ms)}")
            schedule_wake_at(state, unix_ms)
          else
            Logger.info("[nudger:#{state.agent_id}] Overdue alarm — firing now")
            File.rm(path)
            send(self(), :wake_at_alarm)
            state
          end
        rescue
          _ ->
            Logger.warning("[nudger:#{state.agent_id}] Corrupted alarm file — removing")
            File.rm(path)
            state
        end

      {:error, _} ->
        state
    end
  end

  defp clear_persisted_alarm(state) do
    safe_id = sanitize_filename(state.agent_id)
    path = Path.join(@alarm_persist_dir, "#{safe_id}.alarm")
    File.rm(path)
  end

  defp sanitize_filename(name) do
    String.replace(name, ~r/[^a-zA-Z0-9_-]/, "_")
  end

  # ── Internal: message parsing ──────────────────────────────────────────

  defp is_mention?(msg, agent_id) do
    content = extract_content(msg)
    String.contains?(content, "@#{agent_id}")
  end

  defp is_urgent?(msg) do
    content = extract_content(msg) |> String.trim() |> String.downcase()
    String.starts_with?(content, "!urgent")
  end

  defp extract_sender(msg) when is_map(msg) do
    verified = msg["from"] || msg["sender"] || msg[:from] || "unknown"
    original = msg["original_from"]

    if original && original != verified do
      sanitized = original |> sanitize_text() |> truncate(30)
      "#{verified} [via: #{sanitized}]"
    else
      verified
    end
  end

  defp extract_content(msg) when is_map(msg) do
    raw = msg["content"] || msg["body"] || msg["text"] || msg[:content] || ""
    sanitize_text(raw)
  end

  defp sanitize_text(str) when is_binary(str) do
    str
    |> String.replace(~r/\e\[[0-9;]*[A-Za-z]/, "")
    |> String.replace(~r/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/, "")
    |> String.replace(~r/<!(?:CMD|JS)/, "[filtered]")
  end

  defp sanitize_text(_), do: ""

  # ── Internal: helpers ──────────────────────────────────────────────────

  defp now, do: System.system_time(:millisecond)
  defp wall_now, do: DateTime.utc_now() |> DateTime.to_unix(:millisecond)

  defp clamp(val, min_v, max_v), do: max(min_v, min(max_v, val))

  defp truncate(str, max_len) when byte_size(str) <= max_len, do: str
  defp truncate(str, max_len), do: String.slice(str, 0, max_len) <> "..."

  defp format_duration(ms) when ms < 1000, do: "<1s"
  defp format_duration(ms) when ms < 60_000, do: "#{div(ms, 1000)}s"
  defp format_duration(ms) when ms < 3_600_000, do: "#{div(ms, 60_000)}m#{rem(div(ms, 1000), 60)}s"
  defp format_duration(ms), do: "#{div(ms, 3_600_000)}h#{rem(div(ms, 60_000), 60)}m"

  defp format_time(unix_ms) do
    unix_ms
    |> DateTime.from_unix!(:millisecond)
    |> Calendar.strftime("%H:%M:%S UTC")
  end

  defp safe_cast(agent_id, msg) do
    try do
      GenServer.cast(via(agent_id), msg)
    catch
      :exit, _ -> :ok
    end
  end
end
