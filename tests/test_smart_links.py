
import unittest
from utils.injector import DataInjector

class TestSmartLinks(unittest.TestCase):
    def test_conservative_strategy(self):
        outline = [
            {"heading_text": "Introduction", "section_intent": "Intro"},
            {"heading_text": "What is Hostinger?", "section_intent": "Info"},
            {"heading_text": "Features", "section_intent": "Info"},
            {"heading_text": "Performance", "section_intent": "Info"},
            {"heading_text": "Pricing", "section_intent": "Info"},
            {"heading_text": "Conclusion", "section_intent": "Conclusion"},
        ]
        
        urls = [
            {"url": "https://hostinger.com/offer", "anchor_text": "Hostinger Offer", "link_type": "external"},
            {"url": "https://hostinger.com/pricing", "anchor_text": "Pricing Page", "link_type": "external"}
        ]
        
        # Run with conservative strategy
        result = DataInjector.distribute_urls_to_outline(outline, urls, strategy="conservative")
        
        # Verifications
        intro = result[0]
        self.assertIn(urls[0], intro.get("assigned_links", []))
        print(f"✅ Primary Link assigned to: {intro['heading_text']}")
        
        # Check that primary link is NOT in other sections
        for i in range(1, len(result)):
            self.assertNotIn(urls[0], result[i].get("assigned_links", []))
            
        # Check for brand mentions
        mentions_found = 0
        for sec in result[1:]:
            if "Hostinger Offer" in sec.get("brand_mentions", []):
                mentions_found += 1
                print(f"✅ Brand Mention found in: {sec['heading_text']}")
        
        self.assertTrue(mentions_found > 0, "Should have at least one brand mention")
        
        # Check secondary link distribution
        secondary_found = False
        for sec in result:
            if urls[1] in sec.get("assigned_links", []):
                secondary_found = True
                print(f"✅ Secondary Link assigned to: {sec['heading_text']}")
                
        self.assertTrue(secondary_found, "Secondary link should be assigned")

if __name__ == "__main__":
    unittest.main()
