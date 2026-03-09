defmodule Cortex.AgentProcessTest do
  use ExUnit.Case, async: true

  alias Cortex.AgentProcess

  describe "detect_state_from_pane/1 (state detection)" do
    test "detects busy from braille spinner characters" do
      Enum.each(~w(⣾ ⣽ ⣻ ⢿ ⡿ ⣟ ⣯ ⣷), fn spinner ->
        pane = "Working on task... #{spinner}"
        assert AgentProcess.detect_state_from_pane(pane) == :busy
      end)
    end

    test "detects idle from prompt character ❯" do
      assert AgentProcess.detect_state_from_pane("some output\n❯ ") == :idle
    end

    test "detects idle from prompt character >" do
      assert AgentProcess.detect_state_from_pane("some output\n> ") == :idle
    end

    test "detects idle from prompt at end of content" do
      assert AgentProcess.detect_state_from_pane("line1\nline2\n❯") == :idle
    end

    test "busy spinner takes priority over prompt" do
      # Both spinner and prompt present — spinner wins (checked first)
      pane = "Processing ⣾\n❯ "
      assert AgentProcess.detect_state_from_pane(pane) == :busy
    end

    test "detects busy from interactive prompts" do
      assert AgentProcess.detect_state_from_pane("Do you want to proceed?") == :busy
      assert AgentProcess.detect_state_from_pane("Select a model to use") == :busy
      assert AgentProcess.detect_state_from_pane("trust prompt from") == :busy
    end

    test "returns unknown for unrecognized content" do
      assert AgentProcess.detect_state_from_pane("some random output") == :unknown
    end

    test "returns unknown for empty string" do
      assert AgentProcess.detect_state_from_pane("") == :unknown
    end

    test "handles multiline pane content" do
      pane = """
      Line 1 of output
      Line 2 with more text
      Line 3 still going
      ❯\s
      """
      assert AgentProcess.detect_state_from_pane(pane) == :idle
    end
  end

  describe "authorized_for?/2 (authorization)" do
    test "chris has all permissions" do
      Enum.each([:freeze, :unfreeze, :kill, :abort, :peek, :raw, :prompt, :restart], fn cmd ->
        assert AgentProcess.authorized_for?("chris", cmd)
      end)
    end

    test "webbie has limited permissions" do
      assert AgentProcess.authorized_for?("webbie", :freeze)
      assert AgentProcess.authorized_for?("webbie", :unfreeze)
      assert AgentProcess.authorized_for?("webbie", :peek)
      assert AgentProcess.authorized_for?("webbie", :prompt)
      assert AgentProcess.authorized_for?("webbie", :status)
      refute AgentProcess.authorized_for?("webbie", :kill)
      refute AgentProcess.authorized_for?("webbie", :abort)
      refute AgentProcess.authorized_for?("webbie", :restart)
    end

    test "giga has peek and status only" do
      assert AgentProcess.authorized_for?("giga", :peek)
      assert AgentProcess.authorized_for?("giga", :status)
      refute AgentProcess.authorized_for?("giga", :freeze)
      refute AgentProcess.authorized_for?("giga", :kill)
    end

    test "unknown peer has no permissions" do
      refute AgentProcess.authorized_for?("unknown_peer", :peek)
      refute AgentProcess.authorized_for?("unknown_peer", :status)
      refute AgentProcess.authorized_for?("unknown_peer", :freeze)
    end

    test "sandy and rogue have peek and status" do
      for peer <- ["sandy", "rogue"] do
        assert AgentProcess.authorized_for?(peer, :peek)
        assert AgentProcess.authorized_for?(peer, :status)
        refute AgentProcess.authorized_for?(peer, :freeze)
        refute AgentProcess.authorized_for?(peer, :kill)
      end
    end
  end
end
