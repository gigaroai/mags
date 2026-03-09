import Config

if config_env() == :prod do
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise "SECRET_KEY_BASE not set. Generate with: mix phx.gen.secret"

  host = System.get_env("PHX_HOST") || "gigaro.ai"
  port = String.to_integer(System.get_env("CORTEX_PORT") || "443")

  # TLS config (optional — skip for reverse proxy setups)
  tls_opts =
    case {System.get_env("TLS_CERTFILE"), System.get_env("TLS_KEYFILE")} do
      {cert, key} when is_binary(cert) and is_binary(key) ->
        [https: [ip: {0, 0, 0, 0}, port: port, certfile: cert, keyfile: key]]

      _ ->
        [http: [ip: {0, 0, 0, 0}, port: port]]
    end

  config :cortex, CortexWeb.Endpoint,
    [{:url, [host: host, port: port, scheme: "https"]},
     {:server, true},
     {:secret_key_base, secret_key_base}] ++ tls_opts

  config :cortex,
    token_file: System.get_env("CORTEX_TOKENS") || "config/tokens.json"
end
