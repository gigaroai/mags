defmodule CortexWeb.PeerSocketPlug do
  @moduledoc """
  Upgrades incoming HTTP connections at /cortex/ws to WebSocket,
  handing off to CortexWeb.PeerSocket.
  """

  def init(opts), do: opts

  def call(conn, _opts) do
    remote_addr =
      case Plug.Conn.get_req_header(conn, "x-forwarded-for") do
        [addr | _] -> addr
        [] -> to_string(:inet_parse.ntoa(conn.remote_ip))
      end

    conn
    |> WebSockAdapter.upgrade(CortexWeb.PeerSocket, %{remote_addr: remote_addr}, [])
    |> Plug.Conn.halt()
  end
end
