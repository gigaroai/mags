defmodule CortexWeb.DashboardLive do
  use CortexWeb, :live_view

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Phoenix.PubSub.subscribe(Cortex.PubSub, Cortex.Registry.topic())
      # Refresh last_seen display every 5s
      :timer.send_interval(5_000, self(), :tick)
    end

    peers = Cortex.Registry.list_all()

    socket =
      socket
      |> assign(:agents, filter_kind(peers, :agent))
      |> assign(:services, filter_kind(peers, :service))
      |> assign(:events, [])
      |> assign(:now, System.system_time(:millisecond))
      |> assign(:live_connected, connected?(socket))


    {:ok, socket}
  end

  @impl true
  def handle_info({:peer_connected, peer}, socket) do
    peers = Cortex.Registry.list_all()
    event = %{ts: now(), kind: :connected, peer_id: peer.peer_id, peer_kind: peer.peer_kind}

    socket =
      socket
      |> assign(:agents, filter_kind(peers, :agent))
      |> assign(:services, filter_kind(peers, :service))
      |> assign(:events, [event | socket.assigns.events] |> Enum.take(50))

    {:noreply, socket}
  end

  @impl true
  def handle_info({:peer_disconnected, peer}, socket) do
    peers = Cortex.Registry.list_all()
    event = %{ts: now(), kind: :disconnected, peer_id: peer.peer_id, peer_kind: peer.peer_kind}

    socket =
      socket
      |> assign(:agents, filter_kind(peers, :agent))
      |> assign(:services, filter_kind(peers, :service))
      |> assign(:events, [event | socket.assigns.events] |> Enum.take(50))

    {:noreply, socket}
  end

  @impl true
  def handle_info({:peer_updated, _peer}, socket) do
    peers = Cortex.Registry.list_all()

    socket =
      socket
      |> assign(:agents, filter_kind(peers, :agent))
      |> assign(:services, filter_kind(peers, :service))

    {:noreply, socket}
  end

  @impl true
  def handle_info(:tick, socket) do
    {:noreply, assign(socket, :now, System.system_time(:millisecond))}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <header>
      <h1>⬡ <span>CORTEX</span></h1>
      <div class={"status-badge" <> if(@live_connected, do: " connected", else: "")}><%= if(@live_connected, do: "● LIVE", else: "● DISCONNECTED") %></div>
    </header>

    <!-- Stats -->
    <div class="stats">
      <div class="stat-card">
        <div class="label">Agents</div>
        <div class="value agents"><%= length(@agents) %></div>
      </div>
      <div class="stat-card">
        <div class="label">Services</div>
        <div class="value services"><%= length(@services) %></div>
      </div>
      <div class="stat-card">
        <div class="label">Total Peers</div>
        <div class="value total"><%= length(@agents) + length(@services) %></div>
      </div>
      <div class="stat-card">
        <div class="label">Runtime</div>
        <div class="value mono" style="font-size:0.9rem; padding-top:0.5rem;">BEAM/OTP <%= System.otp_release() %></div>
      </div>
    </div>

    <!-- Agents -->
    <div class="section-title">
      <span class="dot agent-color"></span> Agents
    </div>
    <div class="peer-grid">
      <%= if @agents == [] do %>
        <div class="empty-state">No agents connected</div>
      <% else %>
        <%= for agent <- @agents do %>
          <.peer_card peer={agent} now={@now} />
        <% end %>
      <% end %>
    </div>

    <!-- Services -->
    <div class="section-title">
      <span class="dot service-color"></span> Services
    </div>
    <div class="peer-grid">
      <%= if @services == [] do %>
        <div class="empty-state">No services connected</div>
      <% else %>
        <%= for svc <- @services do %>
          <.peer_card peer={svc} now={@now} />
        <% end %>
      <% end %>
    </div>

    <!-- Event Log -->
    <div class="section-title">Event Log</div>
    <div class="event-log">
      <%= if @events == [] do %>
        <div class="entry" style="color: var(--text-dim);">Waiting for events...</div>
      <% else %>
        <%= for event <- @events do %>
          <div class="entry">
            <span class="ts"><%= format_ts(event.ts) %></span>
            <span class={event_class(event.kind)}>
              <%= event_text(event) %>
            </span>
          </div>
        <% end %>
      <% end %>
    </div>
    """
  end

  # ── Components ─────────────────────────────────────────────────────────

  defp peer_card(assigns) do
    ~H"""
    <div class="peer-card">
      <div class="top">
        <span class="peer-id"><%= @peer.peer_id %></span>
        <span class={"state-pill #{@peer.state}"}><%= @peer.state %></span>
      </div>
      <div class="label-text"><%= @peer.label || "—" %></div>
      <div class="meta">
        <span class="k">addr</span>
        <span class="v"><%= @peer.remote_addr %></span>
        <span class="k">session</span>
        <span class="v"><%= @peer.session_id %></span>
        <span class="k">uptime</span>
        <span class="v"><%= format_duration(@now - @peer.connected_at) %></span>
        <span class="k">last seen</span>
        <span class="v"><%= format_ago(@now - @peer.last_seen) %></span>
        <%= if @peer.state_detail do %>
          <span class="k">detail</span>
          <span class="v"><%= @peer.state_detail %></span>
        <% end %>
      </div>
      <%= if @peer.capabilities != [] do %>
        <div class="caps">
          <%= for cap <- @peer.capabilities do %>
            <span class="cap-tag"><%= cap %></span>
          <% end %>
        </div>
      <% end %>
    </div>
    """
  end

  # ── Helpers ────────────────────────────────────────────────────────────

  defp filter_kind(peers, kind) do
    Enum.filter(peers, &(&1.peer_kind == kind))
  end

  defp now, do: System.system_time(:millisecond)

  defp format_ts(ms) do
    ms
    |> DateTime.from_unix!(:millisecond)
    |> Calendar.strftime("%H:%M:%S")
  end

  defp format_duration(ms) when ms < 1000, do: "<1s"
  defp format_duration(ms) when ms < 60_000, do: "#{div(ms, 1000)}s"
  defp format_duration(ms) when ms < 3_600_000, do: "#{div(ms, 60_000)}m #{rem(div(ms, 1000), 60)}s"
  defp format_duration(ms), do: "#{div(ms, 3_600_000)}h #{rem(div(ms, 60_000), 60)}m"

  defp format_ago(ms) when ms < 2000, do: "just now"
  defp format_ago(ms) when ms < 60_000, do: "#{div(ms, 1000)}s ago"
  defp format_ago(ms), do: "#{div(ms, 60_000)}m ago"

  defp event_class(:connected), do: "connected"
  defp event_class(:disconnected), do: "disconnected"
  defp event_class(_), do: "state-change"

  defp event_text(%{kind: :connected, peer_id: id, peer_kind: kind}) do
    "#{id} connected (#{kind})"
  end

  defp event_text(%{kind: :disconnected, peer_id: id}) do
    "#{id} disconnected"
  end

  defp event_text(%{kind: :state_change, peer_id: id, state: state}) do
    "#{id} → #{state}"
  end
end
