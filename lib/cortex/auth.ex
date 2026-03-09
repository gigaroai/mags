defmodule Cortex.Auth do
  @moduledoc """
  Token-based authentication for peers connecting to Cortex.
  Reads tokens from a JSON config file.
  """
  use GenServer
  require Logger

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc "Validate a token. Returns {:ok, token_info} or :invalid."
  def validate(token) do
    GenServer.call(__MODULE__, {:validate, token})
  end

  @doc "Reload tokens from disk."
  def reload do
    GenServer.call(__MODULE__, :reload)
  end

  # ── GenServer ──────────────────────────────────────────────────────────

  @impl true
  def init(_) do
    tokens = load_tokens()
    {:ok, %{tokens: tokens}}
  end

  @impl true
  def handle_call({:validate, token}, _from, state) do
    result =
      case Map.get(state.tokens, token) do
        nil -> :invalid
        info -> {:ok, info}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call(:reload, _from, _state) do
    tokens = load_tokens()
    {:reply, :ok, %{tokens: tokens}}
  end

  # ── Internal ───────────────────────────────────────────────────────────

  defp load_tokens do
    path = Application.get_env(:cortex, :token_file, "config/tokens.json")

    case File.read(path) do
      {:ok, raw} ->
        tokens = Jason.decode!(raw)
        Logger.info("[auth] Loaded #{map_size(tokens)} tokens from #{path}")

        # Normalize keys to atoms for internal use
        tokens
        |> Enum.map(fn {token, info} ->
          {token,
           %{
             peer_id: info["peer_id"],
             peer_kind: String.to_atom(info["peer_kind"]),
             label: info["label"],
             capabilities: info["capabilities"] || []
           }}
        end)
        |> Map.new()

      {:error, reason} ->
        Logger.warning("[auth] Could not read #{path}: #{inspect(reason)}")
        %{}
    end
  end
end
