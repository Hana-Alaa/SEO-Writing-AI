import sys
import os
import asyncio
from pathlib import Path
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.services.strategy_service import StrategyService
from src.services.validation_service import ValidationService

async def test_full_flow():
    strategy_service = StrategyService(None, None, None)
    validation_service = ValidationService(None)
    
    test_cases = [
        {
            "kw": "افضل شركة تصميم مواقع في السعودية",
            "area": "السعودية",
            "expected_head": "شركة",
            "expected_phrase": "شركة تصميم مواقع"
        },
        {
            "kw": "شركة تنظيف منازل في جدة",
            "area": "جدة",
            "expected_head": "شركة",
            "expected_phrase": "شركة تنظيف منازل"
        },
        {
            "kw": "مكتب محاماة في الرياض",
            "area": "الرياض",
            "expected_head": "مكتب",
            "expected_phrase": "مكتب محاماة"
        },
        {
            "kw": "شقق للايجار في الرياض",
            "area": "الرياض",
            "expected_head": "شقق",
            "expected_phrase": "شقق"
        }
    ]
    
    print(f"{'Keyword':<40} | {'Field':<10} | {'Expected':<20} | {'Strategy':<20} | {'Validator':<20}")
    print("-" * 120)
    
    for case in test_cases:
        kw = case["kw"]
        area = case["area"]
        
        # Test Strategy Service
        s_terms = strategy_service._derive_entity_terms(kw, area)
        s_head = s_terms["head"]
        s_phrase = s_terms["phrase"]
        
        # Test Validation Service
        v_profile = validation_service._derive_keyword_profile(kw, area)
        v_head = v_profile["head_entity"]
        v_phrase = v_profile["entity_phrase"]
        
        # Head Check
        print(f"{kw:<40} | {'Head':<10} | {case['expected_head']:<20} | {s_head:<20} | {v_head:<20}")
        # Phrase Check
        print(f"{'':<40} | {'Phrase':<10} | {case['expected_phrase']:<20} | {s_phrase:<20} | {v_phrase:<20}")
        print("-" * 120)

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    asyncio.run(test_full_flow())
