defmodule CortexWeb.PeerSocket do
  @moduledoc """
  Raw WebSocket handler for agent/service connections.
  Each connection gets its own process on the BEAM.

  Protocol:
    1. Client connects to /cortex/ws
    2. Client sends auth message within 10s
    3. On auth_ok, peer is registered and can send/receive messages
    4. Cortex pings every 15s, drops peers silent for 45s
  """
  @behaviour WebSock
  require Logger

  @auth_timeout_ms 10_000
  @ping_interval_ms 15_000
  @ping_timeout_ms 45_000

  # ── WebSock init ───────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    remote_addr = opts[:remote_addr] || "unknown"
    Logger.debug("[ws] New connection from #{remote_addr}")

    # Start auth timeout
    auth_ref = Process.send_after(self(), :auth_timeout, @auth_timeout_ms)

    state = %{
      authed: false,
      session_id: nil,
      peer_id: nil,
      peer_kind: nil,
      remote_addr: remote_addr,
      auth_timer: auth_ref,
      ping_timer: nil,
      last_pong: System.system_time(:millisecond)
    }

    {:ok, state}
  end

  # ── Incoming WebSocket frames ──────────────────────────────────────────

  @impl true
  def handle_in({text, [opcode: :text]}, state) do
    case Jason.decode(text) do
      {:ok, msg} -> handle_message(msg, state)
      {:error, _} -> {:push, {:text, encode_error("invalid_json")}, state}
    end
  end

  def handle_in(_other, state) do
    {:ok, state}
  end

  # ── Process messages (timers, PubSub, etc.) ────────────────────────────

  @impl true
  def handle_info(:auth_timeout, %{authed: false} = state) do
    Logger.debug("[ws] Auth timeout for #{state.remote_addr}")
    {:stop, :normal, {1008, "auth_timeout"}, state}
  end

  def handle_info(:auth_timeout, state) do
    # Already authed, ignore stale timer
    {:ok, state}
  end

  def handle_info(:send_ping, state) do
    now = System.system_time(:millisecond)

    if now - state.last_pong > @ping_timeout_ms do
      Logger.info("[ws] Ping timeout: #{state.peer_id}")
      # FIX v2#5: Don't unregister here — terminate/2 handles it with pid check
      {:stop, :normal, {1008, "ping_timeout"}, state}
    else
      msg = Jason.encode!(%{type: "ping", ts: now})
      timer = Process.send_after(self(), :send_ping, @ping_interval_ms)
      {:push, {:text, msg}, %{state | ping_timer: timer}}
    end
  end

  # Message from another peer (routed through Registry/PubSub)
  def handle_info({:cortex_message, msg}, state) do
    {:push, {:text, Jason.encode!(msg)}, state}
  end

  # v3#17: Removed dead {:nudge_inject, text} handler — injection now uses {:cortex_message, ...}

  # FIX #5: Evicted by a reconnecting peer with the same ID
  def handle_info({:evicted, peer_id}, state) do
    Logger.info("[ws] Evicted: #{peer_id} (new connection took over)")
    # FIX: Unsubscribe from PubSub BEFORE stopping to close the duplicate
    # delivery window between eviction and process termination.
    Phoenix.PubSub.unsubscribe(Cortex.PubSub, Cortex.Registry.topic())
    {:stop, :normal, {1000, "evicted"}, state}
  end

  # ── Topology events from Registry PubSub (agents only) ────────────────

  def handle_info({:peer_connected, peer}, %{authed: true, peer_kind: :agent} = state) do
    # Don't echo our own connection back
    if peer.peer_id != state.peer_id do
      event = %{
        type: "topology_event",
        event: "connected",
        peer: summarize_peer(peer),
        ts: System.system_time(:millisecond)
      }
      {:push, {:text, Jason.encode!(event)}, state}
    else
      {:ok, state}
    end
  end

  def handle_info({:peer_disconnected, peer}, %{authed: true, peer_kind: :agent} = state) do
    event = %{
      type: "topology_event",
      event: "disconnected",
      peer: summarize_peer(peer),
      ts: System.system_time(:millisecond)
    }
    {:push, {:text, Jason.encode!(event)}, state}
  end

  def handle_info({:peer_updated, peer}, %{authed: true, peer_kind: :agent} = state) do
    if peer.peer_id != state.peer_id do
      event = %{
        type: "topology_event",
        event: "state_changed",
        peer: summarize_peer(peer),
        ts: System.system_time(:millisecond)
      }
      {:push, {:text, Jason.encode!(event)}, state}
    else
      {:ok, state}
    end
  end

  def handle_info(_msg, state) do
    {:ok, state}
  end

  # ── Terminate ──────────────────────────────────────────────────────────

  @impl true
  def terminate(_reason, state) do
    # FIX v2#4/#5: Only unregister if WE are still the registered peer (not a replacement)
    if state.session_id do
      case Cortex.Registry.find_by_session(state.session_id) do
        {:ok, peer} when peer.pid == self() ->
          Cortex.Registry.unregister(state.session_id)
        _ ->
          # Already replaced by a new connection, or already cleaned up
          :ok
      end
    end

    :ok
  end

  # ── Message handling ───────────────────────────────────────────────────

  defp handle_message(%{"type" => "auth"} = msg, %{authed: false} = state) do
    token = msg["token"] || ""

    case Cortex.Auth.validate(token) do
      {:ok, token_info} ->
        if state.auth_timer, do: Process.cancel_timer(state.auth_timer)

        # FIX #3: Token is authoritative for identity. Never trust wire peer_id.
        peer_id = token_info.peer_id
        peer_kind_atom = token_info.peer_kind

        # v3#13 FIX: Cap metadata size to prevent memory exhaustion
        raw_metadata = msg["metadata"] || %{}
        metadata =
          case Jason.encode(raw_metadata) do
            {:ok, encoded} when byte_size(encoded) <= 4096 -> raw_metadata
            _ -> %{}
          end

        reg_info = %{
          peer_id: peer_id,
          peer_kind: peer_kind_atom,
          label: token_info.label,
          # FIX v2#10: Token authoritative for capabilities — ignore wire claims
          capabilities: token_info.capabilities,
          pid: self(),
          remote_addr: state.remote_addr,
          metadata: metadata
        }

        # v3#4 FIX: Atomic evict + register (serialized through GenServer, no race)
        {:ok, session_id} = Cortex.Registry.register_with_eviction(peer_id, reg_info)

        # For agents: start AgentManager only if not already managed by AgentUnit
        if peer_kind_atom == :agent and not Cortex.AgentUnit.managed?(peer_id) do
          case Elixir.Registry.lookup(Cortex.AgentManagerRegistry, peer_id) do
            [{pid, _}] ->
              Logger.info("[ws] Stopping orphaned AgentManager for #{peer_id}")
              DynamicSupervisor.terminate_child(Cortex.AgentManagerSupervisor, pid)
            [] -> :ok
          end

          case DynamicSupervisor.start_child(
            Cortex.AgentManagerSupervisor,
            {Cortex.AgentManager, agent_id: peer_id}
          ) do
            {:ok, _pid} -> :ok
            {:error, reason} ->
              Logger.error("[ws] Failed to start AgentManager for #{peer_id}: #{inspect(reason)}")
          end
        end

        # Start heartbeat ping loop
        ping_timer = Process.send_after(self(), :send_ping, @ping_interval_ms)

        reply = %{
          type: "auth_ok",
          session_id: session_id,
          cortex_time: System.system_time(:millisecond)
        }

        new_state = %{
          state
          | authed: true,
            session_id: session_id,
            peer_id: peer_id,
            peer_kind: peer_kind_atom,
            ping_timer: ping_timer,
            last_pong: System.system_time(:millisecond)
        }

        Logger.info("[ws] Authenticated: #{peer_id} (#{peer_kind_atom}) session=#{session_id}")

        # Agents get live topology events (services don't need them)
        if peer_kind_atom == :agent do
          Phoenix.PubSub.subscribe(Cortex.PubSub, Cortex.Registry.topic())
        end

        {:push, {:text, Jason.encode!(reply)}, new_state}

      :invalid ->
        # v3#16 FIX: Close frame (1008, "auth_failed") is the signal to the client.
        # WebSock doesn't support push-then-stop in one return.
        Logger.warning("[ws] Auth failed from #{state.remote_addr}")
        {:stop, :normal, {1008, "auth_failed"}, state}
    end
  end

  defp handle_message(%{"type" => "auth"}, %{authed: true} = state) do
    {:push, {:text, encode_error("already_authenticated")}, state}
  end

  defp handle_message(_msg, %{authed: false} = state) do
    reply = %{type: "auth_fail", reason: "must_authenticate_first"}
    {:push, {:text, Jason.encode!(reply)}, state}
  end

  # ── Authed message types ───────────────────────────────────────────────

  defp handle_message(%{"type" => "pong"}, state) do
    Cortex.Registry.touch(state.session_id)
    {:ok, %{state | last_pong: System.system_time(:millisecond)}}
  end

  defp handle_message(%{"type" => "heartbeat"}, state) do
    Cortex.Registry.touch(state.session_id)
    {:ok, state}
  end

  defp handle_message(%{"type" => "status"} = msg, state) do
    # FIX #4: Whitelist valid states — never String.to_atom on client input
    valid_states = %{
      "idle" => :idle, "busy" => :busy, "stale" => :stale,
      "error" => :error, "shutting_down" => :shutting_down,
      "starting" => :starting
    }

    new_state_str = msg["state"] || "idle"
    new_state_atom = Map.get(valid_states, new_state_str, :idle)
    detail = msg["detail"]
    Cortex.Registry.update_state(state.session_id, new_state_atom, detail)

    # Notify AgentManager of state change (agents only)
    if state.peer_kind == :agent do
      Cortex.AgentManager.agent_state_changed(state.peer_id, new_state_str)
    end

    {:ok, state}
  end

  # Agent nudger config — agent sets idle timeout, cooldown, wake-at
  defp handle_message(%{"type" => "nudger_config"} = msg, state) do
    if state.peer_kind == :agent do
      if ms = msg["idle_timeout_ms"] do
        Cortex.AgentManager.set_idle_timeout(state.peer_id, ms)
      end
      if ms = msg["cooldown_ms"] do
        Cortex.AgentManager.set_cooldown(state.peer_id, ms)
      end
      if unix_ms = msg["wake_at"] do
        Cortex.AgentManager.set_wake_at(state.peer_id, unix_ms)
      end
    end
    {:ok, state}
  end

  # Agent responding to a nudge (Overwatch detects ".\n" and forwards)
  defp handle_message(%{"type" => "nudge_response"} = msg, state) do
    if state.peer_kind == :agent do
      Cortex.AgentManager.agent_responded(state.peer_id, msg["response"] || "")
    end
    {:ok, state}
  end

  # Bridge message relay — ONLY the bridge service can send these
  # FIX #10: Restrict by peer_id, not just message type
  defp handle_message(%{"type" => "bridge_message", "to" => to} = msg, state) do
    if state.peer_id != "bridge" do
      Logger.warning("[ws] Unauthorized bridge_message from #{state.peer_id}")
      {:push, {:text, encode_error("only bridge service can send bridge_message")}, state}
    else
      bridge_msg = %{
        # FIX v2#13: Cortex sender is always "bridge". Original Matrix sender is unverified.
        "from" => state.peer_id,
        "original_from" => msg["from"],
        "content" => msg["content"] || "",
        "channel" => msg["channel"],
        "bridge_seq" => msg["bridge_seq"],
        "ts" => System.system_time(:millisecond),
      }

      Cortex.AgentManager.bridge_message(to, bridge_msg)
      {:ok, state}
    end
  end

  # Agent requests queued messages by ID range
  defp handle_message(%{"type" => "get_messages"} = msg, state) do
    from_id = msg["from_id"] || 0
    to_id = msg["to_id"] || 999_999_999

    msgs = Cortex.AgentManager.get_messages(state.peer_id, from_id, to_id)
    reply = %{type: "messages", messages: msgs}
    {:push, {:text, Jason.encode!(reply)}, state}
  end

  # Point-to-point message routing
  defp handle_message(%{"type" => "message", "to" => to} = msg, state) do
    envelope = %{
      type: "message",
      from: state.peer_id,
      to: to,
      payload: msg["payload"],
      ts: System.system_time(:millisecond)
    }

    case Cortex.Registry.find(to) do
      {:ok, target} ->
        send(target.pid, {:cortex_message, envelope})
        ack = %{type: "message_ack", to: to, status: "delivered"}
        {:push, {:text, Jason.encode!(ack)}, state}

      :not_found ->
        ack = %{type: "message_ack", to: to, status: "not_found"}
        {:push, {:text, Jason.encode!(ack)}, state}
    end
  end

  # Broadcast to peers (defaults to agents only; include services with target: "all")
  defp handle_message(%{"type" => "broadcast"} = msg, state) do
    envelope = %{
      type: "broadcast",
      from: state.peer_id,
      payload: msg["payload"],
      ts: System.system_time(:millisecond)
    }

    # v3#9 FIX: Default to agents + bridge only. "all" includes all services.
    target = msg["target"] || "agents"
    peers = Cortex.Registry.list_all()
      |> Enum.reject(&(&1.session_id == state.session_id))
      |> Enum.filter(fn peer ->
        case target do
          "all" -> true
          _ -> peer.peer_kind == :agent or peer.peer_id == "bridge"
        end
      end)

    Enum.each(peers, fn peer ->
      send(peer.pid, {:cortex_message, envelope})
    end)

    {:ok, state}
  end

  # Service discovery
  defp handle_message(%{"type" => "discover"} = msg, state) do
    results =
      case msg["capability"] do
        nil -> Cortex.Registry.list_by_kind(:service)
        cap -> Cortex.Registry.find_by_capability(cap)
      end
      |> Enum.map(fn svc ->
        %{
          peer_id: svc.peer_id,
          label: svc.label,
          capabilities: svc.capabilities,
          state: svc.state,
          metadata: svc.metadata
        }
      end)

    reply = %{type: "discover_result", services: results, query: msg["capability"]}
    {:push, {:text, Jason.encode!(reply)}, state}
  end

  # Inference request → route to a service
  defp handle_message(%{"type" => "inference_request"} = msg, state) do
    model = msg["model"]
    # Find a service with inference capability that's idle (or least busy)
    services = Cortex.Registry.find_by_capability("inference")

    case pick_service(services, model) do
      nil ->
        reply = %{type: "inference_error", reason: "no_service_available", model: model}
        {:push, {:text, Jason.encode!(reply)}, state}

      target ->
        envelope = %{
          type: "inference_request",
          from: state.peer_id,
          request_id: msg["request_id"] || generate_id(),
          model: model,
          payload: msg["payload"],
          ts: System.system_time(:millisecond)
        }

        send(target.pid, {:cortex_message, envelope})

        ack = %{
          type: "inference_routed",
          request_id: envelope.request_id,
          routed_to: target.peer_id
        }

        {:push, {:text, Jason.encode!(ack)}, state}
    end
  end

  # Inference response — route back to requester
  defp handle_message(%{"type" => "inference_response", "to" => to} = msg, state) do
    envelope = %{
      type: "inference_response",
      from: state.peer_id,
      request_id: msg["request_id"],
      payload: msg["payload"],
      ts: System.system_time(:millisecond)
    }

    case Cortex.Registry.find(to) do
      {:ok, target} -> send(target.pid, {:cortex_message, envelope})
      :not_found -> :ok
    end

    {:ok, state}
  end

  # Full topology query — agent gets complete picture of Cortex state
  defp handle_message(%{"type" => "topology"}, state) do
    handle_message_topology(state)
  end

  # Dependency query — what breaks if a specific peer goes down?
  defp handle_message(%{"type" => "what_if", "peer_id" => target_id}, state) do
    peers = Cortex.Registry.list_all()
    deps = build_dependency_map(peers)

    result = Map.get(deps, target_id, %{error: "peer not found"})

    peer_info =
      case Cortex.Registry.find(target_id) do
        {:ok, p} -> summarize_peer(p)
        :not_found -> %{peer_id: target_id, state: "not_connected"}
      end

    reply = %{
      type: "what_if_result",
      target: peer_info,
      impact: result,
      ts: System.system_time(:millisecond)
    }

    {:push, {:text, Jason.encode!(reply)}, state}
  end

  # Context query — any agent can check peer context% and state
  defp handle_message(%{"type" => "context_query"} = msg, state) do
    result =
      case msg["agent_id"] do
        nil ->
          %{type: "context_result", agents: Cortex.AgentContext.list_all()}

        id ->
          case Cortex.AgentContext.get(id) do
            {:ok, info} ->
              %{type: "context_result", agent: info}

            {:error, :not_found} ->
              %{type: "context_result", error: "not_found", agent_id: id}
          end
      end

    {:push, {:text, Jason.encode!(result)}, state}
  end

  defp handle_message(%{"type" => type}, state) do
    Logger.debug("[ws] Unknown message type '#{type}' from #{state.peer_id}")
    {:ok, state}
  end

  # ── Topology query — full state of Cortex ────────────────────────────

  # Inserted above the catch-all via separate handler clause
  defp handle_message_topology(state) do
    peers = Cortex.Registry.list_all()

    agents = peers |> Enum.filter(&(&1.peer_kind == :agent)) |> Enum.map(&summarize_peer/1)
    services = peers |> Enum.filter(&(&1.peer_kind == :service)) |> Enum.map(&summarize_peer/1)

    # Build dependency map — what breaks if each peer dies
    deps = build_dependency_map(peers)

    reply = %{
      type: "topology",
      agents: agents,
      services: services,
      total_peers: length(peers),
      dependencies: deps,
      cortex: %{
        uptime_seconds: div(:erlang.statistics(:wall_clock) |> elem(0), 1000),
        runtime: "BEAM/OTP #{System.otp_release()}",
      },
      ts: System.system_time(:millisecond)
    }

    {:push, {:text, Jason.encode!(reply)}, state}
  end

  # ── Helpers ────────────────────────────────────────────────────────────

  defp summarize_peer(peer) do
    %{
      peer_id: peer.peer_id,
      kind: peer.peer_kind,
      label: peer.label,
      state: peer.state,
      state_detail: peer.state_detail,
      capabilities: peer.capabilities,
      connected_at: peer.connected_at,
      last_seen: peer.last_seen,
      remote_addr: peer.remote_addr,
      metadata: peer.metadata,
      uptime_ms: System.system_time(:millisecond) - peer.connected_at,
    }
  end

  defp build_dependency_map(peers) do
    # For each peer, compute what capabilities are lost if it disconnects
    # and which other peers depend on it
    peers
    |> Enum.map(fn peer ->
      # Who depends on this peer's capabilities?
      dependents =
        case peer.peer_id do
          "bridge" ->
            # All agents lose Matrix/GMB comms
            agents = peers |> Enum.filter(&(&1.peer_kind == :agent)) |> Enum.map(& &1.peer_id)
            %{affects: agents, loses: ["matrix_comms", "bridge_messages", "gmb"]}

          "meridian" ->
            # All agents lose memory recall/store
            agents = peers |> Enum.filter(&(&1.peer_kind == :agent)) |> Enum.map(& &1.peer_id)
            %{affects: agents, loses: ["memory_recall", "memory_store", "embeddings"]}

          id when id in ["ollama-tau", "ollama-cloud"] ->
            # Agents lose local inference (but may fallback to other ollama)
            other_ollamas =
              peers
              |> Enum.filter(fn p ->
                p.peer_kind == :service and
                "inference" in (p.capabilities || []) and
                p.peer_id != id
              end)

            fallback = Enum.map(other_ollamas, & &1.peer_id)
            agents = peers |> Enum.filter(&(&1.peer_kind == :agent)) |> Enum.map(& &1.peer_id)

            %{
              affects: agents,
              loses: ["inference_from_#{id}"],
              fallback: if(fallback != [], do: fallback, else: nil),
              critical: fallback == []
            }

          _ ->
            if peer.peer_kind == :agent do
              # Agent going down — other agents lose a crew member
              other_agents =
                peers
                |> Enum.filter(&(&1.peer_kind == :agent and &1.peer_id != peer.peer_id))
                |> Enum.map(& &1.peer_id)

              %{affects: other_agents, loses: ["crew_member_#{peer.peer_id}"], critical: false}
            else
              # Generic service
              %{
                affects: peers |> Enum.filter(&(&1.peer_kind == :agent)) |> Enum.map(& &1.peer_id),
                loses: peer.capabilities || [],
              }
            end
        end

      {peer.peer_id, dependents}
    end)
    |> Map.new()
  end

  defp pick_service([], _model), do: nil

  defp pick_service(services, model) do
    # Prefer idle services; if model specified, prefer services advertising that model
    services
    |> Enum.sort_by(fn svc ->
      idle_score = if svc.state == :idle, do: 0, else: 1
      model_score =
        if model && model in (svc.metadata["models"] || []), do: 0, else: 1
      {idle_score, model_score}
    end)
    |> List.first()
  end

  defp generate_id do
    "req-#{System.system_time(:millisecond)}-#{:rand.uniform(999_999)}"
  end

  defp encode_error(reason) do
    Jason.encode!(%{type: "error", reason: reason})
  end
end
