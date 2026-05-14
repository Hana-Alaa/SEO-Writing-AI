import unittest
from unittest.mock import MagicMock, AsyncMock
from src.services.content_generator import SectionWriter

class TestSectionWriterPayloadVisibility(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_ai_client = MagicMock()
        # Mocking the send method as an async method
        self.mock_ai_client.send = AsyncMock(return_value={"content": "Dummy Content", "metadata": {}})
        self.writer = SectionWriter(ai_client=self.mock_ai_client)

    async def test_payload_visibility_in_prompt(self):
        # Create a section with all the new fields
        section = {
            "heading_text": "Prices in Riyadh",
            "taxonomy_axis": "pricing_by_area",
            "preferred_axis": "residential_apartments",
            "forbidden_taxonomy_axis": "commercial_offices",
            "depth_goal": "high_detail_with_local_nuance",
            "observed_data_mentions": ["110,000 SAR", "2,500 Monthly"],
            "must_include_details": ["Detail 1", "Detail 2"],
            "section_promise": "Explain the price ranges clearly",
            "reader_takeaway": "Riyadh prices are rising",
            "practical_decision_value": "Helps you budget accurately"
        }
        
        # We need to provide some other required state for the writer
        market_insights = {}
        article_intent = "informational"
        article_language = "en"
        
        # Call write with correct signature
        await self.writer.write(
            title="Sample Title",
            global_keywords={"primary": "riyadh apartments", "lsi": [], "semantic": []},
            section=section,
            article_intent=article_intent,
            seo_intelligence={"market_analysis": {"market_insights": {}}},
            content_type="informational",
            link_strategy="none",
            brand_url="none",
            brand_link_used=False,
            brand_link_allowed=False,
            allow_external_links=False,
            execution_plan={},
            area="Riyadh"
        )
        
        # Capture the prompt passed to the AI client
        args, kwargs = self.mock_ai_client.send.call_args
        prompt = args[0]
        
        # Verify visibility of all requested fields in the rendered prompt
        fields_to_check = [
            "depth_goal",
            "taxonomy_axis",
            "preferred_axis",
            "forbidden_taxonomy_axis",
            "observed_data_mentions",
            "must_include_details"
        ]
        
        for field in fields_to_check:
            # Check if the field name and its value are in the prompt
            value = section[field]
            if isinstance(value, list):
                for item in value:
                    self.assertIn(item, prompt, f"Field value '{item}' for '{field}' not found in prompt")
            else:
                self.assertIn(value, prompt, f"Field value '{value}' for '{field}' not found in prompt")
            
            # Also check if the field label exists in the prompt (as defined in 02_section_writer_base.txt)
            self.assertIn(f"`{field}`", prompt, f"Field label '`{field}`' not found in prompt")

    async def test_default_payload_for_old_outlines(self):
        # Test that an old section with missing fields still works and uses defaults
        section = {
            "heading_text": "General Info"
        }
        
        # Call write with correct signature
        await self.writer.write(
            title="General Title",
            global_keywords={"primary": "info", "lsi": [], "semantic": []},
            section=section,
            article_intent="informational",
            seo_intelligence={"market_analysis": {"market_insights": {}}},
            content_type="informational",
            link_strategy="none",
            brand_url="none",
            brand_link_used=False,
            brand_link_allowed=False,
            allow_external_links=False,
            execution_plan={},
            area="Global"
        )
        
        # Capture the prompt
        args, _ = self.mock_ai_client.send.call_args
        prompt = args[0]
        
        # Check that labels exist even if values are empty
        self.assertIn("`depth_goal`", prompt)
        self.assertIn("`taxonomy_axis`", prompt)
        self.assertIn("`observed_data_mentions`", prompt)

if __name__ == "__main__":
    unittest.main()
