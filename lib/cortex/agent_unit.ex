defmodule Cortex.AgentUnit do
  @moduledoc """
  Per-agent supervisor using rest_for_one strategy.

  AgentProcess starts first, AgentManager second.
  If AgentProcess crashes, AgentManager restarts too (stale state cleared).
  If AgentManager crashes alone, AgentProcess keeps tmux session alive.
  """
  use Supervisor

  def start_link(opts) do
    agent_id = Keyword.fetch!(opts, :agent_id)
    Supervisor.start_link(__MODULE__, opts, name: via(agent_id))
  end

  def via(agent_id), do: {:via, Registry, {Cortex.AgentUnitRegistry, agent_id}}

  @doc "Check if an agent is managed by an AgentUnit."
  def managed?(agent_id) do
    case Registry.lookup(Cortex.AgentUnitRegistry, agent_id) do
      [{_pid, _}] -> true
      [] -> false
    end
  end

  @impl true
  def init(opts) do
    children = [
      {Cortex.AgentProcess, opts},
      {Cortex.AgentManager, opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  def child_spec(opts) do
    agent_id = Keyword.fetch!(opts, :agent_id)

    %{
      id: {__MODULE__, agent_id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :supervisor,
      shutdown: :infinity
    }
  end
end
