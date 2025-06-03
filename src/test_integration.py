#!/usr/bin/env python3
"""
Quick test script to verify cost tracking integration works.
"""

import sys
import os

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.insert(0, project_root)

def test_cost_calculator():
    """Test the cost calculator."""
    print("Testing CostCalculator...")
    try:
        from src.cost_tracking.cost_calculator import CostCalculator
        calc = CostCalculator()
        
        # Test LLM cost calculation
        result = calc.calculate_llm_cost("gpt-4o-mini", 100, 50)
        print(f"✅ LLM Cost: ${result['total_llm_cost']:.6f}")
        
        # Test compute cost calculation
        compute_result = calc.calculate_cloud_run_cost(2000, 512)
        print(f"✅ Compute Cost: ${compute_result['total_compute_cost']:.8f}")
        
        return True
    except Exception as e:
        print(f"❌ CostCalculator failed: {e}")
        return False

def test_basic_imports():
    """Test that all modules can be imported."""
    print("Testing imports...")
    try:
        from src.cost_tracking import CostCalculator, UsageTracker, SessionSummarizer
        print("✅ All cost tracking modules imported successfully")
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False

def test_orchestrator():
    """Test the orchestrator with cost tracking."""
    print("Testing TwoTierOrchestrator...")
    try:
        from src.MCP.two_tier_orchestrator import TwoTierOrchestrator
        from src.MCP.tool_registry import tool_registry
        
        orchestrator = TwoTierOrchestrator(tool_registry=tool_registry)
        print("✅ TwoTierOrchestrator initialized with cost tracking")
        return True
    except Exception as e:
        print(f"❌ TwoTierOrchestrator failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🧪 Testing Cost Tracking Integration")
    print("=" * 50)
    
    tests = [
        test_basic_imports,
        test_cost_calculator,
        test_orchestrator
    ]
    
    passed = 0
    for test in tests:
        if test():
            passed += 1
        print()
    
    print(f"📊 Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 All tests passed! Cost tracking is ready.")
        print("Try your frontend now - it should work!")
    else:
        print("❌ Some tests failed. Check the errors above.")

if __name__ == "__main__":
    main()
