import unittest
from src.services.workflow_controller import AsyncWorkflowController


class TestPricingExtraction(unittest.TestCase):
    def setUp(self):
        self.controller = object.__new__(AsyncWorkflowController)

    def test_extract_price_from_snippet(self):
        state = {
            "serp_data": {
                "results": [
                    {"title": "Cheap Apartments", "snippet": "Rent starts from 110,000 SAR per year."}
                ]
            }
        }
        mentions = self.controller._extract_observed_pricing_signals(state)
        self.assertTrue(any("110,000" in m for m in mentions))

    def test_extract_arabic_price(self):
        state = {
            "serp_data": {
                "results": [
                    {"title": "شقق للايجار", "snippet": "الايجار الشهري : 2.000 ريال فقط"}
                ]
            }
        }
        mentions = self.controller._extract_observed_pricing_signals(state)
        self.assertTrue(any("2.000" in m for m in mentions))
        self.assertTrue(any("ريال" in m for m in mentions))

    def test_extract_from_top_results_meta_and_headings(self):
        state = {
            "serp_data": {
                "top_results": [
                    {
                        "title": "أسعار شقق للايجار في الرياض",
                        "meta_description": "تبدأ بعض الإيجارات السنوية من 110,000 ريال حسب المنطقة.",
                        "headings": {
                            "h1": "شقق للايجار",
                            "h2": ["متوسط الأسعار 2.000 شهريًا"],
                            "h3": ["إيجار سنوي حسب الحي"],
                        },
                    }
                ]
            }
        }

        mentions = self.controller._extract_observed_pricing_signals(state)

        self.assertTrue(any("110,000" in m for m in mentions))
        self.assertTrue(any("2.000" in m for m in mentions))
        stored = state["seo_intelligence"]["market_analysis"]["market_insights"]["market_data_signals"]["observed_price_mentions"]
        self.assertEqual(mentions, stored)

    def test_preserves_existing_observed_price_mentions(self):
        state = {
            "serp_data": {
                "top_results": [
                    {
                        "meta_title": "Apartment rent prices",
                        "meta_description": "Rent starts from 50,000 SAR yearly.",
                    }
                ]
            },
            "seo_intelligence": {
                "market_analysis": {
                    "market_insights": {
                        "market_data_signals": {
                            "observed_price_mentions": ["previous observed 30,000 SAR"]
                        }
                    }
                }
            },
        }

        mentions = self.controller._extract_observed_pricing_signals(state)

        self.assertIn("previous observed 30,000 SAR", mentions)
        self.assertTrue(any("50,000" in m for m in mentions))

    def test_ignore_price_guide_year_without_currency_context(self):
        state = {
            "serp_data": {
                "top_results": [
                    {
                        "title": "دليل أسعار الشقق 2026",
                        "meta_description": "تعرف على العوامل التي تغير الأسعار.",
                    }
                ]
            }
        }

        mentions = self.controller._extract_observed_pricing_signals(state)

        self.assertFalse(any("2026" in m for m in mentions), mentions)

    def test_ignore_unrelated_numbers(self):
        state = {
            "serp_data": {
                "results": [
                    {"title": "Top 10 Apartments", "snippet": "There are 500 visitors today."}
                ]
            }
        }
        mentions = self.controller._extract_observed_pricing_signals(state)
        self.assertEqual(len(mentions), 0)

    def test_extract_arabic_indic_digits_with_currency(self):
        state = {
            "serp_data": {
                "results": [
                    {"snippet": "متوسط الإيجار ١٠٠٠ جنيه شهريًا في القاهرة"}
                ]
            }
        }
        mentions = self.controller._extract_observed_pricing_signals(state)
        self.assertTrue(any("١٠٠٠" in m or "1000" in m for m in mentions))
        self.assertTrue(any("جنيه" in m for m in mentions))

    def test_extract_short_scale_with_currency(self):
        state = {
            "serp_data": {
                "results": [
                    {"snippet": "Packages start from 110K USD for enterprise clients."}
                ]
            }
        }
        mentions = self.controller._extract_observed_pricing_signals(state)
        self.assertTrue(any("110K" in m for m in mentions))
        self.assertTrue(any("USD" in m for m in mentions))

    def test_extract_million_scale_formats(self):
        state = {
            "serp_data": {
                "results": [
                    {"snippet": "Enterprise rollout from 1.1M EUR with annual support."},
                    {"snippet": "باقات تبدأ من 1.5 مليون جنيه للشركات الكبرى."},
                    {"snippet": "عروض من 250 ألف درهم للمشاريع المتوسطة."},
                ]
            }
        }
        mentions = self.controller._extract_observed_pricing_signals(state)
        self.assertTrue(any("1.1M" in m and "EUR" in m for m in mentions))
        self.assertTrue(any("مليون" in m and "جنيه" in m for m in mentions))
        self.assertTrue(any("ألف" in m and "درهم" in m for m in mentions))

    def test_extract_symbol_prefixed_amount(self):
        state = {
            "serp_data": {
                "results": [
                    {"snippet": "Starter plan at $1,000 per month."},
                    {"snippet": "Localized pricing from 1000 EGP annually."},
                ]
            }
        }
        mentions = self.controller._extract_observed_pricing_signals(state)
        self.assertTrue(any("$1,000" in m or "1,000" in m for m in mentions))
        self.assertTrue(any("1000" in m and "EGP" in m for m in mentions))

    def test_ignore_listing_count_without_currency(self):
        state = {
            "serp_data": {
                "results": [
                    {
                        "title": "شقق للايجار في الرياض",
                        "snippet": "شقق للايجار في الرياض - 329 شقق للايجار | بروبرتي فايندر",
                    }
                ]
            }
        }
        mentions = self.controller._extract_observed_pricing_signals(state)
        self.assertEqual(mentions, [])

    def test_ignore_discount_only_percentage(self):
        state = {
            "serp_data": {
                "results": [
                    {"snippet": "Limited offer: 50% off your first month."},
                    {"snippet": "عرض خاص: خصم ٥٠٪ لفترة محدودة."},
                ]
            }
        }
        mentions = self.controller._extract_observed_pricing_signals(state)
        self.assertEqual(mentions, [])

    def test_pricing_section_receives_mentions(self):
        state = {
            "serp_data": {
                "results": [
                    {"title": "Pricing", "snippet": "Plans start from 50,000 SAR annually."}
                ]
            },
            "article_language": "en",
        }
        self.controller._extract_observed_pricing_signals(state)

        section = {
            "heading_text": "Apartment Pricing",
            "section_type": "pricing",
            "taxonomy_axis": "pricing_by_type",
        }

        enriched = self.controller._enrich_section_contract(
            section=section,
            outline=[section],
            index=0,
            state=state,
        )

        mentions = enriched.get("observed_data_mentions", [])
        self.assertTrue(any("50,000" in m for m in mentions))


if __name__ == "__main__":
    unittest.main()
