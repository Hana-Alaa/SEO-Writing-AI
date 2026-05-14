import unittest
from unittest.mock import MagicMock, AsyncMock
from src.services.workflow_controller import AsyncWorkflowController
import inspect

class TestWorkflowContracts(unittest.TestCase):
    def setUp(self):
        # Mock dependencies to avoid actual API calls
        self.ai_client = MagicMock()
        self.controller = AsyncWorkflowController(ai_client=self.ai_client, work_dir=".")
        
        # Mock internal services
        self.controller.section_writer = MagicMock()
        self.controller.section_writer.write = AsyncMock(return_value={"generated_content": "Test content", "section_id": "sec_01"})
        
    def test_preflight_audit_detects_signature_mismatch(self):
        """Verify that the preflight audit would catch a missing content_type parameter if it were removed."""
        from src.services.content_generator import SectionWriter
        
        sig = inspect.signature(SectionWriter.write)
        self.assertIn("content_type", sig.parameters, "SectionWriter.write MUST have content_type parameter")
        self.assertIn("brand_name", sig.parameters, "SectionWriter.write should have brand_name parameter")

    def test_write_single_section_passes_content_type(self):
        """Verify that _write_single_section passes content_type to the writer."""
        import asyncio
        
        state = {
            "content_type": "brand_commercial",
            "primary_keyword": "test keyword",
            "brand_url": "https://test.com",
            "used_internal_links": [],
            "used_external_links": []
        }
        
        section = {
            "section_id": "sec_01",
            "heading_text": "Test Heading",
            "section_type": "introduction"
        }
        
        # Run the internal helper
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.controller._write_single_section(
            title="Test Title",
            global_keywords={"primary": "test"},
            section=section,
            article_intent="commercial",
            seo_intelligence={},
            content_type="brand_commercial",
            link_strategy={},
            state=state
        ))
        
        # Check call arguments
        args, kwargs = self.controller.section_writer.write.call_args
        self.assertEqual(kwargs.get("content_type"), "brand_commercial")
        self.assertEqual(kwargs.get("title"), "Test Title")

if __name__ == "__main__":
    unittest.main()
