import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.services.strategy_service import StrategyService
from src.services.validation_service import ValidationService

def test_extraction():
    strategy = StrategyService(None, None, None)
    validator = ValidationService(None)
    
    keywords = [
        ("افضل شركة تصميم مواقع في السعودية", "السعودية"),
        ("شركة تنظيف منازل في جدة", "جدة"),
        ("مكتب محاماة في الرياض", "الرياض"),
        ("عيادة أسنان في القاهرة", "القاهرة"),
        ("شقق للايجار في الرياض", "الرياض")
    ]
    
    print(f"{'Keyword':<40} | {'Area':<10} | {'Strategy Phrase':<25} | {'Validator Phrase':<25}")
    print("-" * 110)
    
    for kw, area in keywords:
        s_terms = strategy._derive_entity_terms(kw, area)
        v_terms = validator._derive_keyword_profile(kw, area)
        
        print(f"{kw:<40} | {area:<10} | {s_terms['phrase']:<25} | {v_terms['entity_phrase']:<25}")

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    test_extraction()
