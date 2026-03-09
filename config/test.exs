import Config

config :cortex, CortexWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "test-only-secret-key-base-that-is-at-least-64-bytes-long-for-phoenix-to-accept-it-ok",
  server: false

config :cortex,
  token_file: "config/tokens.json"

config :logger, level: :warning
