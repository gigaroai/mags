defmodule Cortex.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # PubSub for broadcasting registry changes to LiveView
      {Phoenix.PubSub, name: Cortex.PubSub},
      # Peer registry (ETS-backed)
      Cortex.Registry,
      # Agent name registries (for via-tuple naming)
      {Registry, keys: :unique, name: Cortex.AgentManagerRegistry},
      {Registry, keys: :unique, name: Cortex.AgentProcessRegistry},
      {Registry, keys: :unique, name: Cortex.AgentUnitRegistry},
      # Token auth module
      Cortex.Auth,
      # Task supervisor for async tmux operations (B2)
      {Task.Supervisor, name: Cortex.TaskSupervisor},
      # DynamicSupervisor for peer processes
      {DynamicSupervisor, name: Cortex.PeerSupervisor, strategy: :one_for_one},
      # DynamicSupervisor for per-agent units (AgentProcess + AgentManager pairs)
      {DynamicSupervisor, name: Cortex.AgentSupervisor, strategy: :one_for_one},
      # Legacy: DynamicSupervisor for standalone AgentManagers (external peers)
      {DynamicSupervisor, name: Cortex.AgentManagerSupervisor, strategy: :one_for_one},
      # Phoenix endpoint (HTTP + WS)
      CortexWeb.Endpoint
    ]

    opts = [strategy: :one_for_one, name: Cortex.Supervisor]
    result = Supervisor.start_link(children, opts)

    # Start managed agents from config
    start_managed_agents()

    result
  end

  defp start_managed_agents do
    agents = Application.get_env(:cortex, :managed_agents, [])

    Enum.each(agents, fn agent_config ->
      opts = [
        agent_id: agent_config[:agent_id],
        tmux_socket: agent_config[:tmux_socket] || agent_config[:agent_id],
        tmux_session: agent_config[:tmux_session] || agent_config[:agent_id],
        cc_bin: agent_config[:cc_bin] || "claude",
        cc_work_dir: agent_config[:cc_work_dir] || "/home/#{agent_config[:agent_id]}",
        cc_args: agent_config[:cc_args] || ["--dangerously-skip-permissions"],
        idle_poll_ms: agent_config[:idle_poll_ms] || 2_000,
        liveness_poll_ms: agent_config[:liveness_poll_ms] || 5_000,
        stale_threshold_ms: agent_config[:stale_threshold_ms] || 300_000,
        fifo_dir: agent_config[:fifo_dir] || "/home/#{agent_config[:agent_id]}/.overwatch"
      ]

      case DynamicSupervisor.start_child(Cortex.AgentSupervisor, {Cortex.AgentUnit, opts}) do
        {:ok, _pid} ->
          require Logger
          Logger.info("[app] Started managed agent: #{agent_config[:agent_id]}")

        {:error, reason} ->
          require Logger
          Logger.error("[app] Failed to start agent #{agent_config[:agent_id]}: #{inspect(reason)}")
      end
    end)
  end
end
