from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PromptQualityRulesTests(unittest.TestCase):
    def test_intro_hook_quality_rule_exists_in_writer_prompts(self):
        base_prompt = (ROOT / "assets/prompts/templates/02_section_writer_base.txt").read_text(encoding="utf-8")
        commercial_prompt = (ROOT / "assets/prompts/templates/02_section_writer_brand_commercial_v2.txt").read_text(encoding="utf-8")

        self.assertIn("INTRO HOOK QUALITY RULE", base_prompt)
        self.assertIn("concrete reader tension", base_prompt)
        self.assertIn("كثير من الناس", base_prompt)
        self.assertIn("The hook must be editorially specific", commercial_prompt)
        self.assertIn("Do not make the first paragraph a brand pitch", commercial_prompt)

    def test_section_promise_and_table_cap_rules_exist(self):
        base_prompt = (ROOT / "assets/prompts/templates/02_section_writer_base.txt").read_text(encoding="utf-8")
        commercial_prompt = (ROOT / "assets/prompts/templates/02_section_writer_brand_commercial_v2.txt").read_text(encoding="utf-8")

        self.assertIn("COMPOUND HEADING RULE", base_prompt)
        self.assertIn("CHOICE PROMISE RULE", base_prompt)
        self.assertIn("more than **1 table in a single section**", base_prompt)
        self.assertIn("CRITERIA SECTION RULE", base_prompt)
        self.assertIn("do not stop after describing the available types", commercial_prompt.lower())

    def test_metric_rules_do_not_allow_invented_ranges(self):
        base_prompt = (ROOT / "assets/prompts/templates/02_section_writer_base.txt").read_text(encoding="utf-8")

        self.assertIn("Use numeric signals ONLY when they are provided", base_prompt)
        self.assertIn("STRICTLY FORBIDDEN from inventing prices", base_prompt)
        self.assertIn("still fulfill the heading honestly", base_prompt)
        self.assertIn("Each approved H3 is its own promise", base_prompt)
        self.assertNotIn("realistic, helpful estimated ranges", base_prompt)


if __name__ == "__main__":
    unittest.main()
