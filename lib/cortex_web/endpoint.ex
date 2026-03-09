defmodule CortexWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :cortex

  @session_options [
    store: :cookie,
    key: "_cortex_key",
    signing_salt: "c0rt3xSS",
    same_site: "Lax"
  ]

  # LiveView socket for the dashboard
  socket "/cortex/live", Phoenix.LiveView.Socket,
    websocket: [connect_info: [session: @session_options]]

  # Static files — served under /cortex/ prefix for nginx proxy compatibility
  plug Plug.Static,
    at: "/cortex",
    from: :cortex,
    gzip: false,
    only: ~w(assets fonts images favicon.ico robots.txt)

  # Also serve at root for direct access
  plug Plug.Static,
    at: "/",
    from: :cortex,
    gzip: false,
    only: ~w(assets fonts images favicon.ico robots.txt)

  plug Plug.RequestId
  plug Plug.Parsers,
    parsers: [:json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()

  plug Plug.MethodOverride
  plug Plug.Head
  plug Plug.Session, @session_options

  plug CortexWeb.Router

  def static_paths, do: ~w(assets fonts images favicon.ico robots.txt)
end
