defmodule Cortex.Registry do
  @moduledoc """
  ETS-backed registry for connected agents and services.
  Broadcasts changes via PubSub so LiveView stays in sync.
  """
  use GenServer
  require Logger

  @table :cortex_peers
  @pubsub Cortex.PubSub
  @topic "registry"

  # ── Public API ───────────────────────────────────────────────────────────

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc "Register a peer. Called by the Peer process after auth."
  def register(peer_info) do
    GenServer.call(__MODULE__, {:register, peer_info})
  end

  @doc "Unregister a peer by session_id."
  def unregister(session_id) do
    GenServer.cast(__MODULE__, {:unregister, session_id})
  end

  @doc "Update a peer's state."
  def update_state(session_id, state, detail \\ nil) do
    GenServer.cast(__MODULE__, {:update_state, session_id, state, detail})
  end

  @doc "Update last_seen timestamp."
  def touch(session_id) do
    GenServer.cast(__MODULE__, {:touch, session_id})
  end

  @doc "List all connected peers."
  def list_all do
    :ets.tab2list(@table)
    |> Enum.map(fn {_key, record} -> record end)
    |> Enum.sort_by(& &1.connected_at)
  end

  @doc "List peers filtered by kind (:agent or :service)."
  def list_by_kind(kind) do
    list_all() |> Enum.filter(&(&1.peer_kind == kind))
  end

  @doc "Find a peer by peer_id."
  def find(peer_id) do
    # v3#12 FIX: Handle duplicate entries (race condition) without crashing
    case :ets.match_object(@table, {:_, %{peer_id: peer_id}}) do
      [{_key, record} | _rest] -> {:ok, record}
      [] -> :not_found
    end
  end

  @doc "Find a peer by session_id."
  def find_by_session(session_id) do
    case :ets.lookup(@table, session_id) do
      [{_, record}] -> {:ok, record}
      [] -> :not_found
    end
  end

  @doc "Find peers that offer a specific capability."
  def find_by_capability(cap) do
    # v3#3 FIX: Normalize to string comparison to avoid atom/string mismatch
    cap_str = to_string(cap)
    list_all()
    |> Enum.filter(fn rec ->
      Enum.any?(rec[:capabilities] || [], fn c -> to_string(c) == cap_str end)
    end)
  end

  @doc "Get the PubSub topic for registry events."
  def topic, do: @topic

  @doc "Count connected peers."
  def count, do: :ets.info(@table, :size)

  @doc "Register with atomic eviction of stale peer. Prevents race condition."
  def register_with_eviction(peer_id, info) do
    GenServer.call(__MODULE__, {:register_with_eviction, peer_id, info})
  end

  # ── GenServer callbacks ────────────────────────────────────────────────

  @impl true
  def init(_) do
    # v3#11 FIX: :protected — only owning process can write, any can read
    table = :ets.new(@table, [:named_table, :set, :protected, read_concurrency: true])
    {:ok, %{table: table, counter: 0}}
  end

  @impl true
  def handle_call({:register, info}, _from, state) do
    counter = state.counter + 1
    session_id = "sess-#{counter}-#{System.system_time(:millisecond)}"

    record = %{
      session_id: session_id,
      peer_id: info.peer_id,
      peer_kind: info.peer_kind,
      label: info[:label],
      capabilities: info[:capabilities] || [],
      state: :idle,
      state_detail: nil,
      pid: info.pid,
      connected_at: System.system_time(:millisecond),
      last_seen: System.system_time(:millisecond),
      remote_addr: info[:remote_addr] || "unknown",
      metadata: info[:metadata] || %{}
    }

    :ets.insert(@table, {session_id, record})
    Logger.info("[registry] + #{info.peer_id} (#{info.peer_kind}) session=#{session_id}")
    broadcast(:peer_connected, record)

    {:reply, {:ok, session_id}, %{state | counter: counter}}
  end

  # v3#4 FIX: Atomic evict-then-register to prevent race condition
  @impl true
  def handle_call({:register_with_eviction, peer_id, info}, _from, state) do
    # Evict any existing sessions for this peer_id (serialized, no race)
    :ets.match_object(@table, {:_, %{peer_id: peer_id}})
    |> Enum.each(fn {old_sid, old_rec} ->
      Logger.info("[registry] Evicting stale #{peer_id} session=#{old_sid}")
      send(old_rec.pid, {:evicted, peer_id})
      :ets.delete(@table, old_sid)
      broadcast(:peer_disconnected, old_rec)
    end)

    # Register the new session
    counter = state.counter + 1
    session_id = "sess-#{counter}-#{System.system_time(:millisecond)}"

    record = %{
      session_id: session_id,
      peer_id: info.peer_id,
      peer_kind: info.peer_kind,
      label: info[:label],
      capabilities: info[:capabilities] || [],
      state: :idle,
      state_detail: nil,
      pid: info.pid,
      connected_at: System.system_time(:millisecond),
      last_seen: System.system_time(:millisecond),
      remote_addr: info[:remote_addr] || "unknown",
      metadata: info[:metadata] || %{}
    }

    :ets.insert(@table, {session_id, record})
    Logger.info("[registry] + #{info.peer_id} (#{info.peer_kind}) session=#{session_id}")
    broadcast(:peer_connected, record)

    {:reply, {:ok, session_id}, %{state | counter: counter}}
  end

  @impl true
  def handle_cast({:unregister, session_id}, state) do
    case :ets.lookup(@table, session_id) do
      [{_, record}] ->
        :ets.delete(@table, session_id)
        Logger.info("[registry] - #{record.peer_id} session=#{session_id}")
        broadcast(:peer_disconnected, record)

      [] ->
        :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:update_state, session_id, new_state, detail}, state) do
    case :ets.lookup(@table, session_id) do
      [{_, record}] ->
        updated = %{record | state: new_state, state_detail: detail, last_seen: now()}
        :ets.insert(@table, {session_id, updated})
        broadcast(:peer_updated, updated)

      [] ->
        :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:touch, session_id}, state) do
    case :ets.lookup(@table, session_id) do
      [{_, record}] ->
        :ets.insert(@table, {session_id, %{record | last_seen: now()}})

      [] ->
        :ok
    end

    {:noreply, state}
  end

  # ── Internal ───────────────────────────────────────────────────────────

  defp broadcast(event, data) do
    Phoenix.PubSub.broadcast(@pubsub, @topic, {event, data})
  end

  defp now, do: System.system_time(:millisecond)
end
