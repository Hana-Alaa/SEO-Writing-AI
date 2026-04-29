import logging
import sys
import os
from typing import Dict, Any

# Adjust path to import the actual ValidationService
sys.path.append(os.getcwd())
from src.services.validation_service import ValidationService

def test_service_phrase_extraction():
    logging.basicConfig(level=logging.INFO)
    validator = ValidationService()
    
    test_cases = [
        {
            "pk": "افضل شركة تصميم مواقع في السعودية",
            "area": "السعودية",
            "expected_entity": "شركه تصميم مواقع",
            "expected_service": "تصميم مواقع"
        },
        {
            "pk": "افضل شركة تنظيف منازل في جدة",
            "area": "جدة",
            "expected_entity": "شركه تنظيف منازل",
            "expected_service": "تنظيف منازل"
        },
        {
            "pk": "شقق للبيع في دبي",
            "area": "دبي",
            "expected_entity": "شقق",
            "expected_service": "شقق" # Real estate should preserve entity
        }
    ]
    
    print("Running Service Phrase Extraction Tests...")
    for tc in test_cases:
        profile = validator._derive_keyword_profile(tc["pk"], tc["area"])
        print(f"\nKeyword: {tc['pk']}")
        print(f"Entity Phrase: {profile['entity_phrase']}")
        print(f"Service Phrase: {profile['service_phrase']}")
        
        assert profile["entity_phrase"] == tc["expected_entity"]
        assert profile["service_phrase"] == tc["expected_service"]
    
    print("\nAll Tests Passed!")

if __name__ == "__main__":
    test_service_phrase_extraction()
