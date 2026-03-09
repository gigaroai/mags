defmodule Cortex.AgentContext do
  @moduledoc """
  Cross-agent context query API.

  Any agent can check peer context% and state via:
  - Elixir: Cortex.AgentContext.list_all() or .get(agent_id)
  - REST: GET /cortex/api/agents or /cortex/api/agents/:id
  - WebSocket: {"type": "context_query"} or {"type": "context_query", "agent_id": "giga"}
  """

  @doc "List all agents with state, context %, and nudger info."
  def list_all do
    managed = list_managed_agents()
    managed_ids = MapSet.new(Enum.map(managed, & &1.agent_id))

    legacy =
      Cortex.Registry.list_by_kind(:agent)
      |> Enum.reject(fn peer -> MapSet.member?(managed_ids, peer.peer_id) end)
      |> Enum.map(&legacy_summary/1)

    managed ++ legacy
  end

  @doc "Get a single agent's context info."
  def get(agent_id) do
    case get_managed(agent_id) do
      nil -> get_legacy(agent_id)
      info -> {:ok, info}
    end
  end

  # ── Internal ──────────────────────────────────────────────────────────

  defp list_managed_agents do
    Registry.select(Cortex.AgentProcessRegistry, [{{:"$1", :_, :_}, [], [:"$1"]}])
    |> Enum.map(&get_managed/1)
    |> Enum.reject(&is_nil/1)
  end

  defp get_managed(agent_id) do
    process_state = Cortex.AgentProcess.get_state(agent_id)
    manager_state = Cortex.AgentManager.get_state(agent_id)

    if process_state[:agent_state] == :not_running and manager_state[:phase] == :not_running do
      nil
    else
      %{
        agent_id: agent_id,
        managed: true,
        state: process_state[:agent_state] || :unknown,
        context_pct: process_state[:last_context_pct] || 0,
        generation: process_state[:generation],
        frozen: process_state[:frozen] || false,
        tmux_available: process_state[:tmux_available],
        consecutive_tmux_failures: process_state[:consecutive_tmux_failures] || 0,
        last_output_at: process_state[:last_output_at],
        last_state_change: process_state[:last_state_change],
        nudger_phase: manager_state[:phase] || :unknown,
        queued_msgs: manager_state[:queued_msgs] || 0,
        idle_since: manager_state[:idle_since],
        burn_pct: manager_state[:burn_pct] || 0
      }
    end
  end

  defp get_legacy(agent_id) do
    case Cortex.Registry.find(agent_id) do
      {:ok, peer} when peer.peer_kind == :agent ->
        {:ok, legacy_summary(peer)}

      _ ->
        {:error, :not_found}
    end
  end

  defp legacy_summary(peer) do
    manager_state = Cortex.AgentManager.get_state(peer.peer_id)

    %{
      agent_id: peer.peer_id,
      managed: false,
      state: peer.state,
      context_pct: manager_state[:burn_pct] || 0,
      generation: nil,
      frozen: false,
      tmux_available: nil,
      consecutive_tmux_failures: nil,
      last_output_at: nil,
      last_state_change: nil,
      nudger_phase: manager_state[:phase] || :unknown,
      queued_msgs: manager_state[:queued_msgs] || 0,
      idle_since: manager_state[:idle_since],
      burn_pct: manager_state[:burn_pct] || 0,
      connected_at: peer.connected_at,
      last_seen: peer.last_seen
    }
  end
end
