import Config

config :cortex, CortexWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [formats: [json: CortexWeb.ErrorJSON], layout: false],
  pubsub_server: Cortex.PubSub,
  live_view: [signing_salt: "c0rt3xLV"]

config :cortex,
  token_file: "config/tokens.json"

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

config :phoenix, :json_library, Jason

import_config "#{config_env()}.exs"
