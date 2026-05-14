"""
tests/test_taxonomy_axis_planner.py

Unit tests for the taxonomy-axis pre-writing planning layer introduced in
AsyncWorkflowController._plan_taxonomy_axis (workflow_controller.py).

Verified contracts
------------------
1. A pricing section following a category_or_type section receives
   forbidden_taxonomy_axis = "category_or_type".
2. preferred_axis becomes "pricing_by_area" when area/location signals are present.
3. H2 heading_text is NEVER modified by the planner.
4. H3 rewrite is applied ONLY when confirmed identical-segmentation overlap exists
   (>= 50% of current H3s mirror the previous category section's H3s).
   Without confirmed overlap the H3 list is left untouched.
"""

import unittest

from src.services.workflow_controller import AsyncWorkflowController


# ---------------------------------------------------------------------------
# Helper: build a minimal controller without touching AI clients
# ---------------------------------------------------------------------------
def _bare_controller() -> AsyncWorkflowController:
    return object.__new__(AsyncWorkflowController)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
_CATEGORY_SECTION = {
    "section_id": "sec_02",
    "heading_text": "أنواع الشقق للإيجار في الرياض",
    "heading_level": "H2",
    "section_type": "category_or_type",
    "subheadings": ["شقق عزاب", "شقق عوائل", "شقق مفروشة"],
}

_PRICING_SECTION_NO_OVERLAP = {
    "section_id": "sec_03",
    "heading_text": "أسعار الشقق للإيجار في الرياض",
    "heading_level": "H2",
    "section_type": "pricing",
    # H3s do NOT mirror the category section (different segmentation)
    "subheadings": ["عوامل تحديد الإيجار", "نطاق الأسعار العام"],
}

_PRICING_SECTION_CONFIRMED_OVERLAP = {
    "section_id": "sec_04",
    "heading_text": "أسعار الشقق للإيجار في الرياض",
    "heading_level": "H2",
    "section_type": "pricing",
    # H3s are exactly the category section's H3s prefixed with "أسعار"
    "subheadings": ["أسعار شقق العزاب", "أسعار شقق العوائل", "أسعار الشقق المفروشة"],
}

_OUTLINE_BASE = [
    {
        "section_id": "sec_01",
        "heading_text": "مقدمة",
        "heading_level": "INTRO",
        "section_type": "introduction",
        "subheadings": [],
    },
    _CATEGORY_SECTION,
]


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------
class TestTaxonomyAxisPlanner(unittest.TestCase):

    def setUp(self):
        self.ctrl = _bare_controller()
        self._base_state = {
            "primary_keyword": "شقق للإيجار في الرياض",
            "article_language": "ar",
            "content_type": "brand_commercial",
            "area": "الرياض",
            "area_neighborhoods": [],
            "seo_intelligence": {},
            "serp_data": {},
        }

    # -----------------------------------------------------------------------
    # Contract 1 – forbidden_taxonomy_axis = "category_or_type"
    # -----------------------------------------------------------------------
    def test_pricing_after_category_type_sets_forbidden_axis(self):
        """
        When a pricing section follows a category_or_type section,
        the planner MUST set forbidden_taxonomy_axis = "category_or_type".
        """
        section = dict(_PRICING_SECTION_NO_OVERLAP)
        outline = _OUTLINE_BASE + [section]
        state = dict(self._base_state)

        plan = self.ctrl._plan_taxonomy_axis(section, outline, index=2, state=state)

        self.assertEqual(plan["taxonomy_axis"], "pricing")
        self.assertEqual(
            plan["forbidden_taxonomy_axis"],
            "category_or_type",
            "Expected forbidden_taxonomy_axis='category_or_type' when pricing follows "
            "a category_or_type section.",
        )

    # -----------------------------------------------------------------------
    # Contract 2 – preferred_axis = "pricing_by_area" with location signals
    # -----------------------------------------------------------------------
    def test_preferred_axis_is_pricing_by_area_when_area_signal_present(self):
        """
        When area/location signals exist (state['area'] is non-empty),
        preferred_axis must become 'pricing_by_area'.
        """
        section = dict(_PRICING_SECTION_NO_OVERLAP)
        outline = _OUTLINE_BASE + [section]
        state = dict(self._base_state)
        state["area"] = "الرياض"  # explicit area signal

        plan = self.ctrl._plan_taxonomy_axis(section, outline, index=2, state=state)

        self.assertEqual(
            plan["preferred_axis"],
            "pricing_by_area",
            "preferred_axis should be 'pricing_by_area' when area signal is present.",
        )

    def test_preferred_axis_is_pricing_by_type_without_location_signals(self):
        """
        When no area/location signals exist, preferred_axis falls back to
        'pricing_by_type'.
        """
        section = dict(_PRICING_SECTION_NO_OVERLAP)
        # Remove the location_area section from the outline so no implicit signal
        outline = [_OUTLINE_BASE[0], _CATEGORY_SECTION, section]
        state = dict(self._base_state)
        state["area"] = ""  # no area
        state["area_neighborhoods"] = []

        plan = self.ctrl._plan_taxonomy_axis(section, outline, index=2, state=state)

        self.assertEqual(
            plan["preferred_axis"],
            "pricing_by_type",
            "preferred_axis should fall back to 'pricing_by_type' when no location "
            "signals are present.",
        )

    # -----------------------------------------------------------------------
    # Contract 3 – H2 headings are NEVER changed
    # -----------------------------------------------------------------------
    def test_h2_heading_is_never_modified(self):
        """
        The planner must NEVER modify heading_text of any section,
        even when it rewrites H3s for confirmed overlap.
        """
        section = dict(_PRICING_SECTION_CONFIRMED_OVERLAP)
        original_h2 = section["heading_text"]
        outline = _OUTLINE_BASE + [section]
        state = dict(self._base_state)

        # Run full enrichment (which calls _plan_taxonomy_axis internally)
        section["section_contract"] = self.ctrl._build_section_contract(
            section, outline, 2, state
        )
        self.ctrl._enrich_section_contract(section, outline, 2, state)

        self.assertEqual(
            section["heading_text"],
            original_h2,
            "H2 heading_text must not be altered by the taxonomy planner.",
        )

    # -----------------------------------------------------------------------
    # Contract 4a – H3 rewrite on confirmed overlap
    # -----------------------------------------------------------------------
    def test_h3_rewritten_only_on_confirmed_overlap(self):
        """
        When >= 50% of the current pricing H3s mirror the category section's H3s
        (confirmed overlap), the planner must set h3_rewrite_needed=True and
        provide h3_corrected_subheadings.
        """
        section = dict(_PRICING_SECTION_CONFIRMED_OVERLAP)
        outline = _OUTLINE_BASE + [section]
        state = dict(self._base_state)
        state["area"] = "الرياض"

        plan = self.ctrl._plan_taxonomy_axis(section, outline, index=2, state=state)

        self.assertTrue(
            plan.get("h3_rewrite_needed"),
            "h3_rewrite_needed should be True for confirmed H3 overlap.",
        )
        self.assertIn(
            "h3_corrected_subheadings",
            plan,
            "h3_corrected_subheadings must be present when h3_rewrite_needed is True.",
        )
        # Corrected H3s must NOT reuse the old category segmentation
        for new_h3 in plan["h3_corrected_subheadings"]:
            for old_h3 in ["شقق عزاب", "شقق عوائل", "شقق مفروشة"]:
                self.assertNotEqual(
                    new_h3.strip(),
                    old_h3.strip(),
                    f"Corrected H3 '{new_h3}' must not repeat old category H3 '{old_h3}'.",
                )

    # -----------------------------------------------------------------------
    # Contract 4b – NO H3 rewrite without confirmed overlap
    # -----------------------------------------------------------------------
    def test_h3_not_rewritten_without_confirmed_overlap(self):
        """
        When the current pricing H3s use a different segmentation axis
        (< 50% overlap), the planner must NOT rewrite H3s.
        """
        section = dict(_PRICING_SECTION_NO_OVERLAP)
        original_h3s = list(section["subheadings"])
        outline = _OUTLINE_BASE + [section]
        state = dict(self._base_state)
        state["area"] = "الرياض"

        plan = self.ctrl._plan_taxonomy_axis(section, outline, index=2, state=state)

        self.assertFalse(
            plan.get("h3_rewrite_needed"),
            "h3_rewrite_needed must be False when overlap is not confirmed.",
        )
        self.assertNotIn(
            "h3_corrected_subheadings",
            plan,
            "h3_corrected_subheadings must NOT appear when no confirmed overlap.",
        )
        # Original H3s untouched
        self.assertEqual(
            section["subheadings"],
            original_h3s,
            "H3 subheadings must not be modified when overlap is not confirmed.",
        )

    # -----------------------------------------------------------------------
    # Sanity – non-pricing sections are unaffected
    # -----------------------------------------------------------------------
    def test_non_pricing_section_gets_no_forbidden_axis(self):
        """
        Sections that are not 'pricing' must never receive a forbidden_taxonomy_axis,
        regardless of previous sections.
        """
        location_section = {
            "section_id": "sec_03",
            "heading_text": "أفضل أحياء الرياض للإيجار",
            "heading_level": "H2",
            "section_type": "location",
            "subheadings": ["شمال الرياض", "جنوب الرياض"],
        }
        outline = _OUTLINE_BASE + [location_section]
        state = dict(self._base_state)

        plan = self.ctrl._plan_taxonomy_axis(location_section, outline, index=2, state=state)

        self.assertEqual(
            plan["forbidden_taxonomy_axis"],
            "",
            "Non-pricing sections must not receive a forbidden_taxonomy_axis.",
        )


if __name__ == "__main__":
    unittest.main()
