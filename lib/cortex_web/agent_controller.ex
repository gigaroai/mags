defmodule CortexWeb.AgentController do
  use CortexWeb, :controller

  def index(conn, _params) do
    json(conn, %{agents: Cortex.AgentContext.list_all()})
  end

  def show(conn, %{"id" => agent_id}) do
    case Cortex.AgentContext.get(agent_id) do
      {:ok, info} ->
        json(conn, info)

      {:error, :not_found} ->
        conn |> put_status(404) |> json(%{error: "agent not found"})
    end
  end
end
