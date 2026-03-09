defmodule CortexWeb.Router do
  use CortexWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {CortexWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  # WebSocket for agents/services — raw upgrade, not Phoenix Channels
  get "/cortex/ws", CortexWeb.PeerSocketPlug, []

  # LiveView Dashboard (scoped under /cortex for nginx proxy)
  scope "/cortex", CortexWeb do
    pipe_through :browser
    live "/", DashboardLive, :index
  end

  # REST API
  scope "/cortex/api", CortexWeb do
    pipe_through :api
    get "/status", StatusController, :index
    get "/agents", AgentController, :index
    get "/agents/:id", AgentController, :show
  end

  # Redirect bare / to /cortex
  scope "/", CortexWeb do
    pipe_through :browser
    live "/", DashboardLive, :index
  end
end
