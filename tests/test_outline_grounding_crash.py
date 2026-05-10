import inspect
import unittest
from typing import Dict, Any
from unittest.mock import AsyncMock, MagicMock
from src.services.content_generator import OutlineGenerator
from src.utils.contract_safety import validate_service_call


class TestOutlineGeneratorSerPGrounding(unittest.TestCase):
    def test_signature_accepts_serp_outline_brief(self):
        """Verify serp_outline_brief is in the generate() signature (no TypeError)."""
        ai_client = MagicMock()
        gen = OutlineGenerator(ai_client)

        sig = inspect.signature(gen.generate)
        self.assertIn(
            "serp_outline_brief",
            sig.parameters,
            "serp_outline_brief must be a named parameter in OutlineGenerator.generate()",
        )

    def test_validate_service_call_passes_with_brief(self):
        """validate_service_call must not raise when serp_outline_brief is supplied."""
        ai_client = MagicMock()
        gen = OutlineGenerator(ai_client)

        # Should NOT raise PipelineContractError
        try:
            validate_service_call(
                gen.generate,
                title="Test",
                keywords=["test"],
                urls=[],
                article_language="ar",
                intent="informational",
                seo_intelligence={},
                content_type="informational",
                content_strategy={},
                brand_context="",
                area=None,
                serp_outline_brief={"must_consider_sections": ["location", "tickets"]},
            )
        except TypeError as e:
            self.fail(f"validate_service_call raised TypeError: {e}")

    def test_validate_service_call_passes_without_brief(self):
        """validate_service_call must not raise when serp_outline_brief is omitted (optional)."""
        ai_client = MagicMock()
        gen = OutlineGenerator(ai_client)

        try:
            validate_service_call(
                gen.generate,
                title="Test",
                keywords=["test"],
                urls=[],
                article_language="ar",
                intent="informational",
                seo_intelligence={},
                content_type="informational",
                content_strategy={},
                brand_context="",
                area=None,
            )
        except TypeError as e:
            self.fail(f"validate_service_call raised TypeError: {e}")


if __name__ == "__main__":
    unittest.main()
