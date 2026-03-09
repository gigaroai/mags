import Config

config :cortex, CortexWeb.Endpoint,
  http: [ip: {0, 0, 0, 0}, port: 18100],
  check_origin: false,
  code_reloader: false,
  debug_errors: true,
  secret_key_base: "dev-only-secret-key-base-that-is-at-least-64-bytes-long-for-phoenix-to-accept-it-ok",
  watchers: []

config :cortex, dev_routes: true

config :logger, level: :debug
