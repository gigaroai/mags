defmodule CortexWeb.StatusController do
  use CortexWeb, :controller

  def index(conn, _params) do
    peers = Cortex.Registry.list_all()

    payload = %{
      service: "cortex",
      version: "0.1.0",
      runtime: "BEAM/OTP #{System.otp_release()}",
      uptime_seconds: div(System.monotonic_time(:millisecond), 1000),
      agents: peers |> Enum.filter(&(&1.peer_kind == :agent)) |> Enum.map(&summarize/1),
      services: peers |> Enum.filter(&(&1.peer_kind == :service)) |> Enum.map(&summarize/1),
      total_peers: length(peers)
    }

    json(conn, payload)
  end

  defp summarize(peer) do
    %{
      peer_id: peer.peer_id,
      kind: peer.peer_kind,
      label: peer.label,
      state: peer.state,
      capabilities: peer.capabilities,
      connected_at: peer.connected_at,
      last_seen: peer.last_seen,
      remote_addr: peer.remote_addr
    }
  end
end
