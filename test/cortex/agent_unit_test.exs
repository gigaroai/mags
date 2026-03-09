defmodule Cortex.AgentUnitTest do
  use ExUnit.Case

  describe "child_spec/1" do
    test "generates correct child spec" do
      opts = [agent_id: "test_agent"]
      spec = Cortex.AgentUnit.child_spec(opts)

      assert spec.id == {Cortex.AgentUnit, "test_agent"}
      assert spec.start == {Cortex.AgentUnit, :start_link, [opts]}
      assert spec.restart == :permanent
      assert spec.type == :supervisor
      assert spec.shutdown == :infinity
    end
  end

  describe "managed?/1" do
    test "returns false for unmanaged agent" do
      refute Cortex.AgentUnit.managed?("nonexistent_agent_#{:rand.uniform(99999)}")
    end
  end
end
