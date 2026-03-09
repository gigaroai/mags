defmodule Cortex.AgentProcess.Sanitizer do
  @moduledoc """
  Sanitize text before tmux injection. Allowlist approach.

  Strips all control characters except tab (0x09), all ANSI escape
  sequences, and TTB trigger strings. Newlines (0x0A) and carriage
  returns (0x0D) are explicitly stripped to prevent newline injection
  attacks via tmux send-keys.
  """

  # Control chars to strip: 0x00-0x08, 0x0A-0x1F, 0x7F-0x9F
  # Only 0x09 (tab) is preserved — safe for tmux
  # CRITICAL FIX from review NEW-1: includes 0x0A (LF) and 0x0D (CR)
  @control_chars Regex.compile!(~S"[\x00-\x08\x0a-\x1f\x7f-\x9f]")

  # ANSI escape sequences (CSI + OSC)
  @ansi_escapes Regex.compile!(~S"\x1b\[[0-9;]*[a-zA-Z]|\x1b\].*?\x07")

  # TTB trigger (must NEVER appear in injected text)
  @ttb_trigger "<!JS"

  @doc "Sanitize text for safe tmux injection."
  def sanitize(text) when is_binary(text) do
    text
    |> String.normalize(:nfc)
    |> String.replace(@ansi_escapes, "")
    |> String.replace(@control_chars, "")
    |> String.replace(@ttb_trigger, "[filtered]")
    |> String.trim()
  end

  def sanitize(_), do: ""

  @doc "Check if sanitization changed the input (anomaly signal)."
  def was_modified?(original, sanitized), do: original != sanitized
end
