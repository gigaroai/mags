defmodule Cortex.AgentContextTest do
  use ExUnit.Case

  alias Cortex.AgentContext

  describe "list_all/0" do
    test "returns a list" do
      result = AgentContext.list_all()
      assert is_list(result)
    end

    test "each entry has required keys" do
      # With no managed agents, list may be empty — that's fine
      result = AgentContext.list_all()

      Enum.each(result, fn agent ->
        assert Map.has_key?(agent, :agent_id)
        assert Map.has_key?(agent, :managed)
        assert Map.has_key?(agent, :state)
        assert Map.has_key?(agent, :context_pct)
      end)
    end
  end

  describe "get/1" do
    test "returns error for nonexistent agent" do
      assert {:error, :not_found} = AgentContext.get("nonexistent_agent_#{:rand.uniform(99999)}")
    end
  end
end
