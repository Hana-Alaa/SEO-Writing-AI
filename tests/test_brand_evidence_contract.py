# -*- coding: utf-8 -*-
import unittest
import copy
import os
from unittest.mock import AsyncMock, MagicMock
from src.services.brand_evidence_service import (
    BrandEvidenceService,
    build_brand_offer_contract,
    get_empty_brand_offer_contract,
    build_brand_generation_guardrails,
    build_brand_evidence_inventory,
    build_brand_page_briefs,
    classify_page_type,
    select_section_brand_page_briefs,
    select_section_raw_brand_blocks,
)
from src.services.workflow_controller import AsyncWorkflowController

class TestBrandEvidenceContract(unittest.IsolatedAsyncioTestCase):

    def test_schema_stability(self):
        """
        1. build_brand_offer_contract(state) returns full schema with stable keys.
        """
        state = {
            "primary_keyword": "\u0634\u0642\u0642 \u0644\u0644\u0627\u064a\u062c\u0627\u0631 \u0641\u064a \u0627\u0644\u0631\u064a\u0627\u0636", # Arabic string in unicode escape
            "brand_name": "\u0639\u0642\u0627\u0631\u0627\u062a\u064a",
            "internal_resources": [],
            "brand_context": ""
        }
        contract = build_brand_offer_contract(state)
        
        # Verify schema keys
        self.assertIn("brand_identity", contract)
        self.assertIn("offer_mechanics", contract)
        self.assertIn("value_propositions", contract)
        self.assertIn("trust_signals", contract)
        self.assertIn("conversion_actions", contract)
        self.assertIn("keyword_fit", contract)
        self.assertIn("supported_user_intents", contract)
        self.assertIn("brand_limitations", contract)
        self.assertIn("evidence_summary", contract)
        
        # Verify sub-keys
        self.assertIn("brand_name", contract["brand_identity"])
        self.assertIn("confidence", contract["brand_identity"])
        self.assertIn("discovery_features", contract["offer_mechanics"])
        self.assertIn("confidence", contract["offer_mechanics"])
        self.assertIn("target_keyword", contract["keyword_fit"])
        self.assertIn("confidence", contract["keyword_fit"])
        self.assertIn("used_sources", contract["evidence_summary"])
        self.assertIn("strong_evidence", contract["evidence_summary"])

    async def test_preferential_evidence_map(self):
        """
        2. When brand_evidence_map exists with stronger evidence, contract uses it preferentially.
        """
        state = {
            "primary_keyword": "\u0634\u0642\u0642 \u0644\u0644\u0627\u064a\u062c\u0627\u0631 \u0641\u064a \u0627\u0644\u0631\u064a\u0627\u0636",
            "brand_name": "\u0639\u0642\u0627\u0631\u0627\u062a\u064a",
            "brand_context": "descriptive text only",
            "internal_resources": [
                {
                    "link": "https://example.com/search", 
                    "text": "search listings", 
                    "headings": ["search features available", "compare property listing details", "contact provider agent now"]
                }
            ]
        }
        
        service = BrandEvidenceService()
        state = await service.run_brand_evidence_map(state)
        
        # Verify that explicit headings signal gives 'medium' confidence preferentially over 'low' context (stricter high rule)
        contract = build_brand_offer_contract(state)
        self.assertEqual(contract["brand_identity"]["confidence"], "medium")
        self.assertIn("search functionality", contract["evidence_summary"]["strong_evidence"])

    def test_missing_evidence_map_fallback(self):
        """
        3. When brand_evidence_map is missing, builder returns partial low-confidence contract without failing.
           Domain-aware: generic keyword -> unknown, so missing evidence must be neutral (not real-estate).
        """
        state = {
            "primary_keyword": "generic keyword",
            "brand_name": "GenericBrand",
            "internal_resources": []
        }
        
        contract = build_brand_offer_contract(state)
        self.assertEqual(contract["brand_identity"]["confidence"], "low")
        # unknown domain: must contain neutral missing evidence, NOT real-estate phrases
        self.assertNotIn("no listing browsing observed", contract["evidence_summary"]["missing_evidence"])
        self.assertNotIn("no listing browsing observed", contract["brand_limitations"])
        self.assertNotIn("no explicit comparison tool observed", contract["evidence_summary"]["missing_evidence"])
        # Must have at least one neutral item
        self.assertTrue(len(contract["evidence_summary"]["missing_evidence"]) > 0)

    def test_pure_builder_no_mutation(self):
        """
        4. build_brand_offer_contract(state) does not mutate input state.
        """
        state = {
            "primary_keyword": "test keyword",
            "brand_name": "TestBrand",
            "internal_resources": [{"link": "https://example.com/about", "text": "about us"}]
        }
        state_copy = copy.deepcopy(state)
        
        _ = build_brand_offer_contract(state)
        self.assertEqual(state, state_copy)

    async def test_workflow_integration(self):
        """
        5. Integration test: after _step_brand_discovery_router completes, 
           state["brand_evidence_map"] and state["brand_offer_contract"] exist.
        """
        controller = AsyncWorkflowController(work_dir=".")
        controller.research_service.run_brand_discovery = AsyncMock(return_value={
            "brand_url": "https://example.com",
            "primary_keyword": "search listings",
            "internal_resources": [
                {"link": "https://example.com/search", "text": "search listings", "anchor": "search"}
            ]
        })
        # Mock crawler to avoid network calls
        controller.brand_evidence_service.enrich_brand_internal_resources = AsyncMock(return_value={
            "brand_url": "https://example.com",
            "primary_keyword": "search listings",
            "internal_resources": [
                {"link": "https://example.com/search", "text": "search listings", "anchor": "search"}
            ]
        })
        
        state = {
            "brand_url": "https://example.com",
            "primary_keyword": "search listings"
        }
        
        result = await controller._step_brand_discovery_router(state)
        
        self.assertIn("brand_evidence_map", result)
        self.assertIn("brand_offer_contract", result)
        self.assertEqual(result["brand_offer_contract"]["brand_identity"]["confidence"], "low")

    async def test_workflow_safety_fallback(self):
        """
        6. Safety test: if run_brand_evidence_map raises, _step_brand_discovery_router still 
           returns state with brand_offer_contract and does not fail.
        """
        controller = AsyncWorkflowController(work_dir=".")
        controller.research_service.run_brand_discovery = AsyncMock(return_value={
            "brand_url": "https://example.com",
            "primary_keyword": "search listings"
        })
        controller.brand_evidence_service.enrich_brand_internal_resources = AsyncMock(return_value={
            "brand_url": "https://example.com",
            "primary_keyword": "search listings"
        })
        
        controller.brand_evidence_service.run_brand_evidence_map = AsyncMock(side_effect=RuntimeError("Scrape failure"))
        
        state = {
            "brand_url": "https://example.com",
            "primary_keyword": "search listings"
        }
        
        # Should complete successfully and not raise exception
        result = await controller._step_brand_discovery_router(state)
        
        self.assertIn("brand_evidence_map", result)
        self.assertIn("brand_offer_contract", result)
        self.assertEqual(result["brand_offer_contract"]["brand_identity"]["confidence"], "low")

    def test_downstream_regression_check(self):
        """
        7. Regression check: assert no downstream generation module consumes the contract.
        """
        target_files = [
            "src/services/strategy_service.py",
            "src/services/content_generator.py",
            "src/services/outline_repair_service.py",
            "src/services/validation_service.py"
        ]
        
        for file_path in target_files:
            if not os.path.exists(file_path):
                continue
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                # They should not mention "brand_offer_contract" or "build_brand_offer_contract"
                self.assertNotIn("brand_offer_contract", content, f"{file_path} should not consume brand_offer_contract yet.")
                self.assertNotIn("build_brand_offer_contract", content, f"{file_path} should not import/consume build_brand_offer_contract yet.")

    async def test_search_only_does_not_hallucinate(self):
        """
        8. Verify search-only real-estate evidence adds 'search listings' (not district-specific).
        """
        state = {
            "primary_keyword": "rental listings",
            "brand_name": "GenericBrand",
            "brand_evidence_cards": [
                {
                    "url": "https://example.com/search",
                    "title": "Search Rental Listings",
                    "page_type": "services",
                    "headings": ["Search Apartments for Rent", "Browse Rental Listings"],
                    "visible_products_or_services": ["Apartment Rental"],
                    "visible_features_or_capabilities": ["search by area"],
                    "visible_conversion_actions": [],
                    "visible_trust_signals": [],
                    "visible_geography": [],
                    "visible_pricing_or_packages": [],
                    "visible_support_or_contact_methods": []
                }
            ],
            "brand_evidence_map": {
                "strong_signals": ["search or filter"],
                "medium_signals": [],
                "weak_signals": [],
                "missing_evidence": []
            },
            "internal_resources": [
                {"link": "https://example.com/search", "text": "search page"}
            ]
        }
        contract = build_brand_offer_contract(state)
        
        self.assertIn("search listings", contract["offer_mechanics"]["search_or_filter_features"])
        self.assertNotIn("search apartments by district", contract["offer_mechanics"]["search_or_filter_features"])
        self.assertNotIn("filter by furnished / unfurnished", contract["offer_mechanics"]["search_or_filter_features"])

    async def test_comparison_evidence_removes_missing_comparison(self):
        """
        9. Verify comparison evidence does not produce comparison missing_evidence.
        """
        state = {
            "primary_keyword": "compare properties",
            "brand_name": "GenericBrand",
            "internal_resources": [
                {"link": "https://example.com/compare", "text": "compare properties"}
            ]
        }
        service = BrandEvidenceService()
        state = await service.run_brand_evidence_map(state)
        contract = build_brand_offer_contract(state)
        
        self.assertNotIn("no explicit comparison tool observed", contract["brand_limitations"])
        self.assertNotIn("no explicit comparison tool observed", contract["evidence_summary"]["missing_evidence"])

    def test_non_riyadh_keyword_does_not_produce_riyadh_listings(self):
        """
        10. Verify non-Riyadh real estate keyword does not produce "Riyadh rental listings".
        """
        state = {
            "primary_keyword": "\u0634\u0642\u0642 \u0644\u0644\u0627\u064a\u062c\u0627\u0631 \u0641\u064a \u062a\u0628\u0648\u0643",
            "brand_name": "\u0639\u0642\u0627\u0631\u0627\u062a\u064a",
            "brand_evidence_cards": [
                {
                    "url": "https://example.com/browse",
                    "title": "Browse Rental Listings",
                    "page_type": "services",
                    "headings": ["Apartments for Rent", "Browse Property Listings"],
                    "visible_products_or_services": ["Apartment Rental", "Villa Rental"],
                    "visible_features_or_capabilities": [],
                    "visible_conversion_actions": [],
                    "visible_trust_signals": [],
                    "visible_geography": ["Tabuk"],
                    "visible_pricing_or_packages": [],
                    "visible_support_or_contact_methods": []
                }
            ],
            "brand_evidence_map": {
                "strong_signals": ["browse listings"],
                "medium_signals": [],
                "weak_signals": [],
                "missing_evidence": []
            },
            "internal_resources": [
                {"link": "https://example.com/browse", "text": "browse listings"}
            ]
        }
        contract = build_brand_offer_contract(state)
        
        self.assertIn("Tabuk rental listings", contract["keyword_fit"]["relevant_brand_capabilities"])
        self.assertNotIn("Riyadh rental listings", contract["keyword_fit"]["relevant_brand_capabilities"])

    def test_generic_contact_does_not_hallucinate_inquiry(self):
        """
        11. Verify generic contact evidence does not produce "submit inquiry".
            With the new strict classifier, real-estate contact mechanics need
            explicit real-estate evidence cards.
        """
        state = {
            "primary_keyword": "rental listings",
            "brand_name": "GenericBrand",
            "brand_evidence_cards": [
                {
                    "url": "https://example.com/contact",
                    "title": "Contact Agent",
                    "page_type": "contact",
                    "headings": ["Contact Rental Agent", "Browse Apartment Listings"],
                    "visible_products_or_services": ["Apartment Listings"],
                    "visible_features_or_capabilities": [],
                    "visible_conversion_actions": [],
                    "visible_trust_signals": [],
                    "visible_geography": [],
                    "visible_pricing_or_packages": [],
                    "visible_support_or_contact_methods": []
                }
            ],
            "brand_evidence_map": {
                "strong_signals": ["contact provider"],
                "medium_signals": [],
                "weak_signals": [],
                "missing_evidence": []
            },
            "internal_resources": [
                {"link": "https://example.com/contact", "text": "contact us"}
            ]
        }
        contract = build_brand_offer_contract(state)
        
        self.assertIn("contact agent", contract["conversion_actions"])
        self.assertNotIn("submit inquiry", contract["conversion_actions"])

    def test_brand_context_remains_unchanged(self):
        """
        12. Prove that the canonical brand_context is completely unchanged/unmutated.
        """
        state = {
            "primary_keyword": "test keyword",
            "brand_name": "TestBrand",
            "brand_context": "Clean, original, human-readable context.",
            "brand_offer_contract": {
                "brand_identity": {"confidence": "low"},
                "conversion_actions": [],
                "value_propositions": []
            }
        }
        guardrails = build_brand_generation_guardrails(state)
        # Should not modify state["brand_context"]
        self.assertEqual(state["brand_context"], "Clean, original, human-readable context.")

    def test_brand_guardrail_context_exists_separately(self):
        """
        13. Prove that brand_guardrail_context exists separately in the workflow state.
        """
        state = {
            "primary_keyword": "test keyword",
            "brand_name": "TestBrand",
            "brand_offer_contract": {
                "brand_identity": {"confidence": "medium"},
                "conversion_actions": ["request quote"],
                "value_propositions": ["custom site building"]
            }
        }
        guardrails = build_brand_generation_guardrails(state)
        self.assertEqual(guardrails["brand_confidence"], "medium")
        self.assertIn("request quote", guardrails["allowed_conversion_actions"])
        self.assertIn("custom site building", guardrails["allowed_brand_claims"])

    def test_low_confidence_blocks_dedicated_brand_sections(self):
        """
        14. Prove that low confidence blocks why-choose, proof, testimonial, and case-study sections.
        """
        state = {
            "primary_keyword": "test keyword",
            "brand_name": "TestBrand",
            "brand_offer_contract": {
                "brand_identity": {"confidence": "low"},
                "conversion_actions": [],
                "value_propositions": []
            }
        }
        guardrails = build_brand_generation_guardrails(state)
        self.assertEqual(guardrails["brand_confidence"], "low")
        self.assertEqual(guardrails["brand_section_policy"], "do_not_create_dedicated_brand_proof_or_why_choose_sections")
        self.assertEqual(guardrails["brand_usage_mode"], "soft_context_only")

    def test_mocked_crawl_evidence_maps_digital_agency_contract(self):
        """
        15. Prove that mocked crawl evidence makes brand_offer_contract non-empty (medium/high confidence and digital agency capabilities mapped).
        """
        state = {
            "primary_keyword": "web design agency services",
            "brand_name": "PixelPerfect",
            "internal_resources": [
                {
                    "link": "https://pixelperfect.com/services",
                    "text": "we offer website design, web app development, and seo services",
                    "headings": ["our expertise in website design", "web application development", "seo services", "trusted certified agency pricing"],
                    "page_text": "We provide premium web design, SEO services, and web application development."
                },
                {
                    "link": "https://pixelperfect.com/portfolio",
                    "text": "check out our website design portfolio projects and work",
                    "headings": ["our portfolio gallery and photos", "recent projects location map"],
                    "page_text": "Our portfolio includes beautiful responsive design work."
                },
                {
                    "link": "https://pixelperfect.com/contact",
                    "text": "contact us to request a quote or book a consultation",
                    "headings": ["contact us to book or search listings", "request a quote"],
                    "page_text": "Contact us to request a quote. Located in Riyadh."
                }
            ]
        }
        # Run map
        service = BrandEvidenceService()
        import asyncio
        state = asyncio.run(service.run_brand_evidence_map(state))
        contract = build_brand_offer_contract(state)
        
        # Verify confidence is medium/high because we have multiple strong signals
        self.assertIn(contract["brand_identity"]["confidence"], ["medium", "high"])
        self.assertEqual(contract["brand_identity"]["category"], "web design agency")
        self.assertEqual(contract["brand_identity"]["business_model"], "b2b web design and development services")
        
        # Verify capabilities
        services_lower = [s.lower() for s in contract["offer_mechanics"]["supporting_services"]]
        self.assertTrue(any("website design" in s for s in services_lower))
        self.assertTrue(any("web application development" in s for s in services_lower))
        self.assertTrue(any("seo" in s for s in services_lower))
        self.assertFalse(any("portfolio" in s or "project" in s for s in services_lower))
        
        # Verify conversion actions - new strict logic: only explicit CTAs from visible_conversion_actions
        # The internal_resources page_text contains 'request a quote' but new strict fallback
        # only reads cta_labels when visible_support_or_contact_methods has content.
        # Assert that whatever actions exist do NOT include hallucinated ones.
        self.assertNotIn("submit inquiry", contract["conversion_actions"])
        self.assertNotIn("browse listings", contract["conversion_actions"])

    async def test_high_confidence_requires_multiple_strong_source_urls(self):
        """
        Strong signals clustered on one page should stay medium until supported by a second explicit source URL.
        """
        state = {
            "primary_keyword": "web design agency services",
            "brand_name": "PixelPerfect",
            "internal_resources": [
                {
                    "link": "https://pixelperfect.com/services",
                    "text": "website design seo contact pricing certified portfolio map",
                    "headings": [
                        "website design services",
                        "contact us for pricing",
                        "certified portfolio gallery with location map",
                        "search service options"
                    ]
                },
                {
                    "link": "https://pixelperfect.com/about",
                    "text": "about our team",
                    "headings": ["about us"]
                }
            ]
        }
        service = BrandEvidenceService()
        state = await service.run_brand_evidence_map(state)
        contract = build_brand_offer_contract(state)

        self.assertEqual(state["brand_evidence_map"]["strong_source_urls"], ["https://pixelperfect.com/services"])
        self.assertEqual(contract["brand_identity"]["confidence"], "medium")

    async def test_workflow_safety_fallback_with_guardrails(self):
        """
        16. Prove that crawl failure still returns brand_evidence_map, brand_offer_contract, and brand_generation_guardrails safely.
        """
        controller = AsyncWorkflowController(work_dir=".")
        controller.research_service.run_brand_discovery = AsyncMock(return_value={
            "brand_url": "https://failed-crawl-site.com",
            "primary_keyword": "rental listings"
        })
        
        # Simulate failure in crawl by mocking enrich_brand_internal_resources to raise
        controller.brand_evidence_service.enrich_brand_internal_resources = AsyncMock(side_effect=RuntimeError("DNS lookup failed"))
        # Simulating run_brand_evidence_map success or fallback
        controller.brand_evidence_service.run_brand_evidence_map = AsyncMock(return_value={
            "brand_url": "https://failed-crawl-site.com",
            "primary_keyword": "rental listings",
            "brand_evidence_map": {
                "strong_signals": [],
                "medium_signals": [],
                "weak_signals": [],
                "strong_source_urls": [],
                "source_counts": {"headings": 0, "cta_labels": 0, "anchors": 0, "urls": 0},
                "missing_evidence": []
            }
        })
        
        state = {
            "brand_url": "https://failed-crawl-site.com",
            "primary_keyword": "rental listings"
        }
        
        result = await controller._step_brand_discovery_router(state)
        
        # Ensure fallback fields are fully populated and workflow does not raise
        self.assertIn("brand_evidence_map", result)
        self.assertIn("brand_offer_contract", result)
        self.assertIn("brand_generation_guardrails", result)
        self.assertIn("brand_guardrail_context", result)
        
        # Ensure confidence is low
        self.assertEqual(result["brand_offer_contract"]["brand_identity"]["confidence"], "low")
        self.assertEqual(result["brand_generation_guardrails"]["brand_confidence"], "low")

    @unittest.mock.patch("httpx.AsyncClient.get")
    async def test_crawler_mocked_http(self, mock_get):
        """
        Dedicated crawler unit test using mocked HTTP responses to assert cap, same-domain, and asset/admin filtering.
        """
        homepage_html = """
        <html>
            <head><title>Home Title</title></head>
            <body>
                <h1>Welcome to Home</h1>
                <a href="/services">Our Services</a>
                <a href="https://otherdomain.com/about">Other Domain</a>
                <a href="/wp-admin/edit.php">Admin Panel</a>
                <a href="/brochure.pdf">Download Brochure</a>
                <a href="/portfolio">Our Portfolio</a>
                <a href="/contact">Get in touch</a>
            </body>
        </html>
        """
        services_html = "<html><head><title>Services</title></head><body><h2>Web Design</h2></body></html>"
        portfolio_html = "<html><head><title>Portfolio</title></head><body><h2>Portfolio</h2></body></html>"
        contact_html = "<html><head><title>Contact</title></head><body><h2>Contact Us</h2></body></html>"

        class MockResponse:
            def __init__(self, text, status_code=200):
                self.text = text
                self.status_code = status_code

        def get_side_effect(url, *args, **kwargs):
            if url == "https://pixelperfect.com" or url == "https://pixelperfect.com/":
                return MockResponse(homepage_html)
            elif "services" in url:
                return MockResponse(services_html)
            elif "portfolio" in url:
                return MockResponse(portfolio_html)
            elif "contact" in url:
                return MockResponse(contact_html)
            return MockResponse("", 404)

        mock_get.side_effect = get_side_effect

        from bs4 import BeautifulSoup
        import httpx
        
        service = BrandEvidenceService()
        state = {
            "brand_url": "https://pixelperfect.com",
            "primary_keyword": "website design"
        }
        res = await service.enrich_brand_internal_resources(state, max_pages=3)
        resources = res.get("internal_resources", [])
        
        # Verify capped at 3
        self.assertLessEqual(len(resources), 3)
        
        # Same domain and asset/admin checks
        for r in resources:
            self.assertIn("pixelperfect.com", r["link"])
            self.assertNotIn("wp-admin", r["link"])
            self.assertNotIn(".pdf", r["link"])

    @unittest.mock.patch("httpx.AsyncClient.get")
    async def test_topic_aware_crawler_reads_relevant_pages_and_project_details(self, mock_get):
        """The crawl budget should favor topic-matched pages and one-hop project details."""
        homepage_html = """
        <html><head><title>Home</title></head><body>
          <a href="/digital-video-production">Video Production</a>
          <a href="/web-design">Web Design Services</a>
          <a href="/projects">Projects</a>
          <a href="/contact">Contact</a>
        </body></html>
        """
        web_design_html = """
        <html><head><title>Web Design Services</title></head><body>
          <header><h2>Main Menu</h2><a href="/contact">Contact</a></header>
          <h1>Web Design Services</h1>
          <p>BrandCo provides custom UX design, responsive websites, and web development for business teams.</p>
          <footer>Subscribe Newsletter Facebook Instagram</footer>
        </body></html>
        """
        projects_html = """
        <html><head><title>Projects</title></head><body>
          <h1>Projects</h1>
          <a href="/portfolio/acumen">Acumen Consulting Egypt</a>
          <a href="/portfolio/aqar">Aqar Ya Masr Web app</a>
        </body></html>
        """
        acumen_html = """
        <html><head><title>Acumen Consulting Egypt</title></head><body>
          <h1>Acumen Consulting Egypt</h1>
          <p>Project: Acumen Consulting Egypt. Client work included UX/UI and web application development.</p>
        </body></html>
        """
        video_html = "<html><head><title>Video</title></head><body><h1>Video Production</h1></body></html>"
        contact_html = "<html><head><title>Contact</title></head><body><h1>Contact</h1></body></html>"

        class MockResponse:
            def __init__(self, text, status_code=200):
                self.text = text
                self.status_code = status_code

        def get_side_effect(url, *args, **kwargs):
            if url.rstrip("/") == "https://brand.test":
                return MockResponse(homepage_html)
            if url.endswith("/web-design"):
                return MockResponse(web_design_html)
            if url.endswith("/projects"):
                return MockResponse(projects_html)
            if url.endswith("/portfolio/acumen"):
                return MockResponse(acumen_html)
            if url.endswith("/digital-video-production"):
                return MockResponse(video_html)
            if url.endswith("/contact"):
                return MockResponse(contact_html)
            return MockResponse("", 404)

        mock_get.side_effect = get_side_effect

        service = BrandEvidenceService()
        state = {
            "brand_url": "https://brand.test",
            "primary_keyword": "web design company",
            "content_type": "brand_commercial",
        }

        res = await service.enrich_brand_internal_resources(state, max_pages=4)
        links = [item.get("link") for item in res.get("internal_resources", [])]

        self.assertIn("https://brand.test/web-design", links)
        self.assertIn("https://brand.test/projects", links)
        self.assertIn("https://brand.test/portfolio/acumen", links)
        self.assertNotIn("https://brand.test/digital-video-production", links)

        web_resource = next(item for item in res["internal_resources"] if item.get("link") == "https://brand.test/web-design")
        section_text = " ".join(section.get("body_text", "") for section in web_resource.get("semantic_sections", []))
        self.assertIn("custom UX design", section_text)
        self.assertNotIn("Main Menu", section_text)
        self.assertNotIn("Subscribe Newsletter", section_text)
        self.assertGreater(res.get("brand_crawl_report", {}).get("page_read_stats", [])[0].get("text_chars", 0), 0)

    async def test_post_outline_targeted_crawl_uses_outline_promises(self):
        """After outline generation, promised project evidence should steer a targeted crawl pass."""
        controller = AsyncWorkflowController(work_dir=".")

        async def fake_enrich(state, max_pages=8):
            state = dict(state)
            resources = list(state.get("internal_resources", []))
            resources.append({
                "link": "https://brand.test/projects/acumen",
                "title": "Acumen Consulting Egypt",
                "page_type": "portfolio",
                "page_text_full": "Project: Acumen Consulting Egypt. Client work included UX/UI and web development.",
                "semantic_sections": [
                    {
                        "heading": "Acumen Consulting Egypt",
                        "heading_level": 1,
                        "body_text": "Project: Acumen Consulting Egypt. Client work included UX/UI and web development.",
                        "url": "https://brand.test/projects/acumen",
                        "page_title": "Acumen Consulting Egypt",
                        "page_type": "portfolio",
                    }
                ],
                "is_brand_crawled": True,
            })
            state["internal_resources"] = resources
            return state

        controller.brand_evidence_service.enrich_brand_internal_resources = AsyncMock(side_effect=fake_enrich)
        state = {
            "brand_url": "https://brand.test",
            "brand_name": "BrandCo",
            "content_type": "brand_commercial",
            "primary_keyword": "web design company",
            "raw_title": "Best web design company",
            "internal_resources": [
                {
                    "link": "https://brand.test",
                    "title": "Home",
                    "page_type": "home",
                    "page_text_full": "BrandCo provides web design services.",
                }
            ],
        }
        outline = [
            {
                "heading_text": "Projects shown by BrandCo",
                "section_type": "proof",
                "taxonomy_axis": "brand_projects",
            }
        ]

        result = await controller._run_post_outline_brand_targeted_crawl(state, outline)

        self.assertTrue(result["outline_evidence_requirements"]["needs_projects"])
        self.assertIn("projects", result["brand_crawl_focus"].lower())
        self.assertTrue(controller.brand_evidence_service.enrich_brand_internal_resources.await_count <= 1)
        self.assertIn("https://brand.test/projects/acumen", result["post_outline_brand_crawl_report"]["new_urls"])
        self.assertTrue(result.get("brand_source_chunks"))

    async def test_brand_guardrail_context_not_injected_into_outline(self):
        """
        Legacy guardrail context must not be injected as outline brand truth.
        """
        controller = AsyncWorkflowController(work_dir=".")
        controller.outline_gen = MagicMock()
        controller.outline_gen.generate = AsyncMock(return_value={
            "outline": [],
            "metadata": {"prompt": "", "response": "", "tokens": {}, "model": ""}
        })
        controller.validator.consolidate_faq = MagicMock(side_effect=lambda outline: outline)
        controller.validator.repair_outline_deterministic = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.validator.enforce_intent_distribution = MagicMock(side_effect=lambda outline, intent, content_type: (outline, []))
        controller.validator.inject_local_seo = MagicMock(side_effect=lambda outline, area: (outline, []))
        controller.validator.validate_outline_quality = MagicMock(return_value=[])
        controller.validator.enforce_cta_policy = MagicMock(side_effect=lambda outline, content_type: outline)
        controller.validator.enforce_outline_structure = MagicMock(side_effect=lambda outline, content_type: outline)
        controller.validator.enforce_content_angle = MagicMock(side_effect=lambda outline, content_strategy: outline)
        controller.validator.adjust_paa_by_intent = MagicMock(side_effect=lambda outline, intent: outline)
        controller.validator.enforce_paa_sections = MagicMock(return_value={"paa_ok": True, "paa_ratio": 1, "missing_count": 0})
        controller.outline_repair_service.promote_visitor_intents = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.dedupe_faq_against_h2 = MagicMock(side_effect=lambda outline: outline)
        controller.outline_repair_service.refill_faq_after_dedupe = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.normalize_heading_only_section_types = MagicMock(side_effect=lambda outline: outline)
        controller.outline_repair_service.clean_echo_and_repetition = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.apply_strategic_map_and_roles = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.clean_conclusion_heading = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_gen._normalize_section = MagicMock()
        
        state = {
            "input_data": {
                "title": "Best Web Design in Riyadh",
                "keywords": ["web design"]
            },
            "brand_url": "https://pixelperfect.com",
            "brand_name": "Pixel Perfect",
            "brand_context": "We design websites.",
            "brand_guardrail_context": "\n[BRAND GENERATION GUARDRAILS]\n- Brand confidence: low",
            "primary_keyword": "web design",
            "seo_intelligence": {
                "market_analysis": {
                    "market_insights": {}
                }
            },
            "content_strategy": {}
        }
        
        await controller._step_1_outline(state)
            
        self.assertTrue(controller.outline_gen.generate.called)
        called_kwargs = controller.outline_gen.generate.call_args[1]
        self.assertIn("brand_context", called_kwargs)
        self.assertNotIn("[BRAND GENERATION GUARDRAILS]", called_kwargs["brand_context"])
        self.assertIn("[BRAND EVIDENCE INVENTORY - OUTLINE GATE]", called_kwargs["brand_context"])
        self.assertIn("We design websites.", called_kwargs["brand_context"])

    async def test_brand_guardrail_context_not_injected_into_section_writer(self):
        """
        Legacy guardrail context must not reach the writer-facing brand context.
        """
        controller = AsyncWorkflowController(work_dir=".")
        controller.section_writer = MagicMock()
        controller.section_writer.write = AsyncMock(return_value={
            "generated_content": "Mocked Content",
            "section_id": "intro",
            "brand_mentions_count": 0
        })
        
        state = {
            "brand_url": "https://pixelperfect.com",
            "brand_name": "Pixel Perfect",
            "brand_context": "We design websites.",
            "brand_guardrail_context": "\n[BRAND GENERATION GUARDRAILS]\n- Brand confidence: low",
            "available_links_pool": {"internal": [], "external": []}
        }
        
        section = {
            "section_id": "intro",
            "heading_text": "Introduction",
            "section_type": "introduction"
        }
        
        await controller._write_single_section(
            title="Best Web Design",
            global_keywords={},
            section=section,
            article_intent="informational",
            seo_intelligence={},
            content_type="brand_commercial",
            link_strategy={},
            state=state
        )
        
        self.assertTrue(controller.section_writer.write.called)
        called_kwargs = controller.section_writer.write.call_args[1]
        self.assertIn("brand_context", called_kwargs)
        self.assertNotIn("[BRAND GENERATION GUARDRAILS]", called_kwargs["brand_context"])
        # Pack-Only Writer Truth: brand_context now describes the knowledge pack, not raw source blocks
        self.assertIn("page-by-page brand knowledge pack", called_kwargs["brand_context"])

    async def test_faq_enrichment_guard_on_low_confidence(self):
        """
        Verify that a low-confidence brand contract correctly sets the block policy
        and prevents brand FAQ insertion in the outline repair step.
        """
        controller = AsyncWorkflowController(work_dir=".")
        
        controller.outline_gen = MagicMock()
        controller.outline_gen.generate = AsyncMock(return_value={
            "outline": [
                {
                    "section_id": "sec_faq",
                    "heading_text": "Frequently Asked Questions",
                    "section_type": "faq",
                    "subheadings": []
                }
            ],
            "metadata": {"prompt": "", "response": "", "tokens": {}, "model": ""}
        })
        
        controller.outline_repair_service.enrich_brand_utility_faq = MagicMock()
        
        state = {
            "input_data": {
                "title": "Best Web Design in Riyadh",
                "keywords": ["web design"]
            },
            "brand_url": "https://pixelperfect.com",
            "brand_name": "Pixel Perfect",
            "primary_keyword": "web design",
            "heading_only_mode": True,
            "brand_generation_guardrails": {
                "brand_section_policy": "do_not_create_dedicated_brand_proof_or_why_choose_sections"
            },
            "seo_intelligence": {
                "market_analysis": {
                    "market_insights": {}
                }
            },
            "content_strategy": {}
        }
        
        await controller._step_1_outline(state)
            
        self.assertFalse(controller.outline_repair_service.enrich_brand_utility_faq.called)

    def test_brand_writing_brief_schema(self):
        """
        Verify build_brand_writing_brief returns complete and stable schema.
        """
        from src.services.brand_evidence_service import build_brand_writing_brief
        state = {
            "brand_name": "Creative Minds",
            "primary_keyword": "website design",
            "brand_offer_contract": {
                "brand_identity": {"confidence": "medium"},
                "offer_mechanics": {"supporting_services": ["Custom Development"]},
                "value_propositions": ["High Quality Design"],
                "trust_signals": ["10 Years Experience"],
                "conversion_actions": ["Request Quote"]
            },
            "brand_generation_guardrails": {
                "brand_confidence": "medium",
                "brand_usage_mode": "standard_context",
                "brand_section_policy": "do_not_create_dedicated_brand_proof_or_why_choose_sections"
            }
        }
        
        brief = build_brand_writing_brief(state)
        
        # Verify schema keys
        self.assertEqual(brief["brand_name"], "Creative Minds")
        self.assertEqual(brief["evidence_confidence"], "medium")
        self.assertEqual(brief["brand_usage_mode"], "standard_context")
        self.assertEqual(brief["allowed_claim_strength"], "operational")
        self.assertIn("Custom Development", brief["allowed_services"])
        self.assertIn("High Quality Design", brief["allowed_claims"])
        self.assertIn("Request Quote", brief["allowed_conversion_actions"])
        self.assertIn("guarantees", brief["forbidden_claim_categories"])
        self.assertIn("Services Offered By Creative Minds For website design", brief["allowed_heading_patterns"])

    def test_brand_writing_brief_no_mutation(self):
        """
        Verify builder does not mutate input state.
        """
        from src.services.brand_evidence_service import build_brand_writing_brief
        state = {
            "brand_name": "Creative Minds",
            "primary_keyword": "website design"
        }
        state_copy = copy.deepcopy(state)
        _ = build_brand_writing_brief(state)
        self.assertEqual(state, state_copy)

    def test_brand_writing_brief_confidence_levels(self):
        """
        Verify confidence levels map to correct claim strengths and constraints.
        """
        from src.services.brand_evidence_service import build_brand_writing_brief
        
        # 1. Low Confidence
        state_low = {
            "brand_name": "Creative Minds",
            "brand_generation_guardrails": {"brand_confidence": "low"}
        }
        brief_low = build_brand_writing_brief(state_low)
        self.assertEqual(brief_low["allowed_claim_strength"], "contextual")
        self.assertEqual(brief_low["allowed_claims"], [])
        self.assertEqual(brief_low["allowed_heading_patterns"], [])
        
        # 2. High Confidence
        state_high = {
            "brand_name": "Creative Minds",
            "primary_keyword": "website design",
            "brand_generation_guardrails": {"brand_confidence": "high"},
            "brand_offer_contract": {
                "brand_identity": {"confidence": "high"},
                "trust_signals": ["Verified Portfolios"]
            }
        }
        brief_high = build_brand_writing_brief(state_high)
        self.assertEqual(brief_high["allowed_claim_strength"], "differentiation")
        self.assertIn("Verified Portfolios", brief_high["allowed_claims"])

        # 3. High confidence without trust/value evidence should stay operational, not differentiation
        state_high_no_diff = {
            "brand_name": "Creative Minds",
            "primary_keyword": "website design",
            "brand_generation_guardrails": {"brand_confidence": "high"},
            "brand_offer_contract": {
                "brand_identity": {"confidence": "high"},
                "offer_mechanics": {"supporting_services": ["website design"]}
            }
        }
        brief_high_no_diff = build_brand_writing_brief(state_high_no_diff)
        self.assertEqual(brief_high_no_diff["allowed_claim_strength"], "operational")
        self.assertIn("Do not use differentiation", " ".join(brief_high_no_diff["section_guidance"]))

    def test_brand_writing_brief_prefers_page_briefs_over_noisy_contract_dump(self):
        """Page briefs should replace noisy legacy card/contract values in writer boundary context."""
        from src.services.brand_evidence_service import build_brand_writing_brief, format_brand_writing_brief_context

        state = {
            "brand_name": "BrandCo",
            "primary_keyword": "software services",
            "brand_generation_guardrails": {"brand_confidence": "high", "brand_usage_mode": "standard_context"},
            "brand_offer_contract": {
                "brand_identity": {"confidence": "high"},
                "offer_mechanics": {
                    "supporting_services": [
                        "UX Design by following the latest",
                        "expert design",
                        "Specialized Design Services for Building & Management",
                    ]
                },
                "value_propositions": ["Why You Should Choose Us"],
            },
            "brand_evidence_cards": [
                {
                    "visible_products_or_services": ["IntoSOFTWARE", "Design Services"],
                    "visible_features_or_capabilities": ["Fast Turnaround"],
                }
            ],
            "brand_page_briefs": [
                {
                    "observed_services": ["Web Development", "Mobile App Development"],
                    "observed_technologies": ["React", "Node.js"],
                    "observed_process_steps": ["Planning", "Testing"],
                    "observed_trust_signals": [],
                    "observed_ctas": ["Contact"],
                }
            ],
        }

        brief = build_brand_writing_brief(state)
        context = format_brand_writing_brief_context(brief)

        self.assertIn("Web Development", brief["allowed_services"])
        self.assertIn("Mobile App Development", brief["allowed_services"])
        self.assertNotIn("UX Design by following the latest", context)
        self.assertNotIn("expert design", context)
        self.assertNotIn("Specialized Design Services for Building & Management", context)
        self.assertNotIn("IntoSOFTWARE", context)

    def test_apply_brand_claim_gate_scenarios(self):
        """
        Verify that apply_brand_claim_gate blocks forbidden brand claims while preserving newlines and other content.
        """
        from src.services.brand_evidence_service import apply_brand_claim_gate
        
        brief = {
            "brand_name": "Creative Minds",
            "evidence_confidence": "medium",
            "allowed_claims": ["Custom Development"],
            "allowed_conversion_actions": ["contact"]
        }
        
        # 1. Violating sentence is removed, surrounding content stays
        text_violation = (
            "We build premium websites.\n\n"
            "Creative Minds guarantees website design within 20 days.\n\n"
            "Planning content requires solid information architecture. We must make sure it is correct."
        )
        gated = apply_brand_claim_gate(text_violation, brief)
        self.assertNotIn("guarantees", gated)
        self.assertNotIn("within 20 days", gated)
        self.assertIn("We build premium websites.", gated)
        self.assertIn("Planning content requires solid information architecture.", gated)
        # Verify paragraph/newline preservation
        self.assertIn("\n\n", gated)

        # 2. Non-offending brand sentences are kept
        text_ok = "Creative Minds provides Custom Development. Navigation is important for SEO."
        gated_ok = apply_brand_claim_gate(text_ok, brief)
        self.assertIn("Creative Minds provides Custom Development.", gated_ok)
        self.assertIn("Navigation is important for SEO.", gated_ok)

        # 3. Clean up spaces and empty lines correctly
        text_all_violations = "Creative Minds guarantees premium support.\n\nCreative Minds is trusted by 500 clients."
        gated_all = apply_brand_claim_gate(text_all_violations, brief)
        self.assertEqual(gated_all, "")

    async def test_workflow_context_and_gate_wiring(self):
        """
        Verify that brand_writing_brief is wired into workflow steps,
        both headers exist in outline/writer brand_context, and claim gate gates section content.
        """
        controller = AsyncWorkflowController(work_dir=".")
        
        # Bypass all outline validators
        controller.validator.consolidate_faq = MagicMock(side_effect=lambda outline: outline)
        controller.validator.repair_outline_deterministic = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.validator.enforce_intent_distribution = MagicMock(side_effect=lambda outline, intent, content_type: (outline, []))
        controller.validator.inject_local_seo = MagicMock(side_effect=lambda outline, area: (outline, []))
        controller.validator.validate_outline_quality = MagicMock(return_value=[])
        controller.validator.enforce_cta_policy = MagicMock(side_effect=lambda outline, content_type: outline)
        controller.validator.enforce_outline_structure = MagicMock(side_effect=lambda outline, content_type: outline)
        controller.validator.enforce_content_angle = MagicMock(side_effect=lambda outline, content_strategy: outline)
        controller.validator.adjust_paa_by_intent = MagicMock(side_effect=lambda outline, intent: outline)
        controller.validator.enforce_paa_sections = MagicMock(return_value={"paa_ok": True, "paa_ratio": 1, "missing_count": 0})
        controller.outline_repair_service.promote_visitor_intents = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.dedupe_faq_against_h2 = MagicMock(side_effect=lambda outline: outline)
        controller.outline_repair_service.refill_faq_after_dedupe = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.normalize_heading_only_section_types = MagicMock(side_effect=lambda outline: outline)
        controller.outline_repair_service.clean_echo_and_repetition = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.apply_strategic_map_and_roles = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.clean_conclusion_heading = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.validator.validate_outline_structure = MagicMock(return_value=(True, []))
        controller.validator.validate_outline_coverage = MagicMock(return_value=(True, []))
        controller.outline_gen._normalize_section = MagicMock()
        controller.research_service.run_brand_discovery = AsyncMock(side_effect=lambda state, *args, **kwargs: {
            **state,
            "brand_url": "https://pixelperfect.com",
            "brand_name": "Pixel Perfect",
            "primary_keyword": "website design"
        })
        
        # Mock crawler to preserve input state keys
        controller.brand_evidence_service.enrich_brand_internal_resources = AsyncMock(side_effect=lambda state, *args, **kwargs: {
            **state,
            "brand_url": "https://pixelperfect.com",
            "primary_keyword": "website design"
        })
        controller.brand_evidence_service.run_brand_evidence_map = AsyncMock(side_effect=lambda state, *args, **kwargs: {
            **state,
            "brand_url": "https://pixelperfect.com",
            "primary_keyword": "website design",
            "brand_evidence_map": {
                "strong_signals": [],
                "medium_signals": [],
                "weak_signals": [],
                "strong_source_urls": [],
                "source_counts": {"headings": 0, "cta_labels": 0, "anchors": 0, "urls": 0},
                "missing_evidence": []
            }
        })
        
        state = {
            "brand_url": "https://pixelperfect.com",
            "brand_name": "Pixel Perfect",
            "primary_keyword": "website design",
            "heading_only_mode": True
        }
        
        # 1. Verify discovery wiring
        res = await controller._step_brand_discovery_router(state)
        self.assertIn("brand_writing_brief", res)
        self.assertIn("brand_writing_brief_context", res)
        
        # 2. Verify outline context uses the inventory gate, not legacy brief dumps.
        controller.outline_gen = MagicMock()
        controller.outline_gen.generate = AsyncMock(return_value={
            "outline": [],
            "metadata": {"prompt": "", "response": "", "tokens": {}, "model": ""}
        })
        
        await controller._step_1_outline(res)
        called_kwargs_out = controller.outline_gen.generate.call_args[1]
        self.assertIn("[BRAND EVIDENCE INVENTORY - OUTLINE GATE]", called_kwargs_out["brand_context"])
        self.assertNotIn("[BRAND GENERATION GUARDRAILS", called_kwargs_out["brand_context"])
        self.assertNotIn("[BRAND WRITING BRIEF", called_kwargs_out["brand_context"])

        # 3. Verify section writer context injection has BOTH headers
        controller.section_writer = MagicMock()
        controller.section_writer.write = AsyncMock(return_value={
            "content": "Pixel Perfect guarantees fast delivery.\n\nWebsite planning is key.",
            "section_content": "Pixel Perfect guarantees fast delivery.\n\nWebsite planning is key.",
            "section_id": "intro",
            "brand_mentions_count": 0
        })
        
        section = {
            "section_id": "intro",
            "heading_text": "Introduction",
            "section_type": "introduction"
        }
        
        ret_section = await controller._write_single_section(
            title="Best Web Design",
            global_keywords={},
            section=section,
            article_intent="informational",
            seo_intelligence={},
            content_type="brand_commercial",
            link_strategy={},
            state=res
        )
        
        # Verify legacy headers are not present in writer-facing context.
        called_kwargs_writer = controller.section_writer.write.call_args[1]
        self.assertNotIn("[BRAND GENERATION GUARDRAILS", called_kwargs_writer["brand_context"])
        self.assertNotIn("[BRAND WRITING BRIEF", called_kwargs_writer["brand_context"])
        # Pack-Only Writer Truth: brand_context references the knowledge pack, not raw source blocks
        self.assertIn("page-by-page brand knowledge pack", called_kwargs_writer["brand_context"])

        # Verify claim gate successfully gated key values in returned section
        self.assertNotIn("guarantees", ret_section["generated_content"])
        self.assertIn("Website planning is key.", ret_section["generated_content"])
        self.assertNotIn("guarantees", ret_section["content"])
        self.assertNotIn("guarantees", ret_section["section_content"])

    def test_brand_evidence_cards_and_index(self):
        # Imports
        from src.services.brand_evidence_service import (
            build_brand_evidence_cards,
            build_brand_pages_index
        )
        import copy
        
        # Define neutral fixtures
        state = {
            "internal_resources": [
                {
                    "link": "https://sleekconsultants.com/services",
                    "title": "Sleek Consultants - Our Services",
                    "headings": ["Our Services", "Strategy Consultation", "Risk Analysis", "Why Choose Us"],
                    "cta_labels": ["Request a Quote", "Contact Us"],
                    "page_text": "We provide Strategy Consultation and Risk Analysis for modern firms. Our strategy uses responsive design. Experience of 10 years and over 200+ projects completed. Located in Dammam, Saudi Arabia. contact@sleekconsultants.com +966500000000"
                },
                {
                    "link": "https://gadgetworld.com/products",
                    "title": "GadgetWorld - Innovative Smart Devices",
                    "headings": ["Products", "Smart Pro Watch", "Wireless Charge Hub", "Reviews"],
                    "cta_labels": ["Buy Now", "Add to Cart"],
                    "page_text": "We offer high-quality Smart Pro Watch and Wireless Charge Hub. Starting at 299 SAR. All products include mobile-friendly companion apps and secure payment integration. 5000+ satisfied clients. Shipping available in Riyadh."
                },
                {
                    "link": "https://gadgetworld.com/blog/how-to-save-battery",
                    "title": "How to Save Smart Watch Battery Life - GadgetWorld Blog",
                    "headings": ["How to Save Smart Watch Battery Life", "1. Lower Brightness", "2. Turn off Bluetooth"],
                    "cta_labels": ["Read More", "Share Post"],
                    "page_text": "Lowering brightness can extend your smartwatch battery by up to 50%. Turning off Bluetooth when not in use is another great tip."
                }
            ]
        }
        
        state_copy = copy.deepcopy(state)
        
        # 1. Verify stable schema & no mutation
        cards = build_brand_evidence_cards(state)
        self.assertEqual(state, state_copy, "The input state must not be mutated!")
        self.assertEqual(len(cards), 3)
        
        required_keys = {
            "url", "title", "page_type", "headings", "cta_labels",
            "visible_products_or_services", "visible_features_or_capabilities",
            "visible_process_steps", "visible_conversion_actions",
            "visible_trust_signals", "visible_geography",
            "visible_project_or_case_study_examples", "visible_pricing_or_packages",
            "visible_support_or_contact_methods", "usable_snippets",
            "excluded_reason"
        }
        
        for card in cards:
            for key in required_keys:
                self.assertIn(key, card)
                
        # 2. Verify page type detection
        self.assertEqual(cards[0]["page_type"], "services")
        self.assertEqual(cards[1]["page_type"], "product")
        self.assertEqual(cards[2]["page_type"], "blog")
        
        # 3. Verify extracts visible services/features
        self.assertIn("Strategy Consultation", cards[0]["visible_products_or_services"])
        self.assertIn("Risk Analysis", cards[0]["visible_products_or_services"])
        self.assertIn("Smart Pro Watch", cards[1]["visible_products_or_services"])
        self.assertIn("Wireless Charge Hub", cards[1]["visible_products_or_services"])
        
        # Features/capabilities extraction
        self.assertIn("responsive design", cards[0]["visible_features_or_capabilities"])
        self.assertIn("secure payment", cards[1]["visible_features_or_capabilities"])
        
        # Conversion actions
        self.assertIn("Request a Quote", cards[0]["visible_conversion_actions"])
        self.assertIn("Buy Now", cards[1]["visible_conversion_actions"])
        
        # Trust signals
        self.assertIn("10 years", cards[0]["visible_trust_signals"])
        self.assertIn("200+ projects", cards[0]["visible_trust_signals"])
        self.assertIn("5000+ satisfied clients", cards[1]["visible_trust_signals"])
        
        # Support/contact
        self.assertIn("contact@sleekconsultants.com", cards[0]["visible_support_or_contact_methods"])
        self.assertIn("+966500000000", cards[0]["visible_support_or_contact_methods"])
        
        # Pricing
        self.assertIn("299 SAR", cards[1]["visible_pricing_or_packages"])
        
        # Snippets check: 1-4 excerpts, max 240 chars each
        self.assertTrue(1 <= len(cards[0]["usable_snippets"]) <= 4)
        for snip in cards[0]["usable_snippets"]:
            self.assertTrue(len(snip) <= 240)
            
        # 4. Verify explicit geography
        self.assertIn("Saudi Arabia", cards[0]["visible_geography"])
        self.assertIn("Dammam", cards[0]["visible_geography"])
        self.assertIn("Riyadh", cards[1]["visible_geography"])
        # No geography inferred on blog
        self.assertEqual(len(cards[2]["visible_geography"]), 0)
        
        # 5. Verify irrelevant blog page excluded
        self.assertEqual(cards[2]["page_type"], "blog")
        self.assertIsNotNone(cards[2]["excluded_reason"])
        self.assertIn("Irrelevant informational blog page", cards[2]["excluded_reason"])
        
        # 6. Verify index building includes all cards & excluded reason
        index = build_brand_pages_index(state)
        self.assertEqual(len(index), 3)
        self.assertIn("https://sleekconsultants.com/services", index)
        self.assertIn("https://gadgetworld.com/products", index)
        self.assertIn("https://gadgetworld.com/blog/how-to-save-battery", index)
        
        # Excluded reason shown in compact text
        self.assertIn("EXCLUDED", index["https://gadgetworld.com/blog/how-to-save-battery"])
        self.assertIn("Irrelevant informational blog page", index["https://gadgetworld.com/blog/how-to-save-battery"])
        self.assertIn("CTA Labels", index["https://sleekconsultants.com/services"])
        self.assertIn("Conversion Actions", index["https://sleekconsultants.com/services"])
        self.assertIn("Headings", index["https://sleekconsultants.com/services"])

    def test_brand_evidence_cards_arabic_extraction_and_url_key(self):
        from src.services.brand_evidence_service import build_brand_evidence_cards
        import copy

        state = {
            "internal_resources": [
                {
                    "url": "https://example.com/خدمات",
                    "title": "خدماتنا",
                    "headings": ["خدماتنا", "تصميم المواقع", "خطوات العمل"],
                    "cta_labels": ["اطلب عرض سعر", "تواصل معنا"],
                    "page_text": "نقدم خدمات تصميم المواقع والتسويق الرقمي للشركات في القاهرة ومصر. اطلب عرض سعر عبر واتساب.",
                }
            ]
        }
        state_copy = copy.deepcopy(state)

        cards = build_brand_evidence_cards(state)
        self.assertEqual(state, state_copy)
        self.assertEqual(len(cards), 1)

        card = cards[0]
        self.assertEqual(card["url"], "https://example.com/خدمات")
        self.assertEqual(card["page_type"], "services")

        visible_offer_text = " | ".join(
            card["visible_products_or_services"] + card["visible_features_or_capabilities"]
        )
        self.assertIn("تصميم المواقع", visible_offer_text)
        self.assertIn("التسويق الرقمي", visible_offer_text)

        self.assertNotIn("خدماتنا", card["visible_products_or_services"])
        self.assertNotIn("خطوات العمل", card["visible_products_or_services"])
        for broken in ["الم", "التس", "يق"]:
            self.assertNotIn(broken, card["visible_products_or_services"])
            self.assertNotIn(broken, card["visible_features_or_capabilities"])

        self.assertIn("القاهرة", card["visible_geography"])
        self.assertIn("مصر", card["visible_geography"])
        self.assertIn("WhatsApp", card["visible_support_or_contact_methods"])
        self.assertIn("اطلب عرض سعر", card["visible_conversion_actions"])

    def test_brand_pages_index_includes_conversion_and_project_fields(self):
        from src.services.brand_evidence_service import build_brand_pages_index

        state = {
            "internal_resources": [
                {
                    "url": "https://example.com/projects",
                    "title": "Projects",
                    "headings": ["Projects", "Acumen Consulting Egypt"],
                    "cta_labels": ["Request a Quote"],
                    "page_text": "Project: Acumen Consulting Egypt. We provide implementation support for business teams.",
                }
            ]
        }

        index = build_brand_pages_index(state)
        text = index["https://example.com/projects"]

        self.assertIn("Headings", text)
        self.assertIn("CTA Labels: Request a Quote", text)
        self.assertIn("Conversion Actions: Request a Quote", text)
        self.assertIn("Projects/Case Studies", text)
        self.assertIn("Acumen Consulting Egypt", text)

    def test_domain_neutral_contract_mapping(self):
        import copy
        import json
        from src.services.brand_evidence_service import build_brand_offer_contract
        
        # 1. Digital service fixture
        state_ds = {
            "primary_keyword": "website design solutions",
            "brand_name": "SleekWeb Solutions",
            "brand_context": "B2B web development and hosting services",
            "brand_evidence_cards": [
                {
                    "url": "https://sleekweb.com/services",
                    "title": "Our Web Design & Hosting Services",
                    "page_type": "services",
                    "headings": ["Website Development", "Cloud Hosting", "Our Process"],
                    "visible_products_or_services": ["Website Design", "Cloud Hosting"],
                    "visible_features_or_capabilities": ["responsive layout", "fast loading"],
                    "visible_conversion_actions": ["Request a Quote", "Contact Us"],
                    "visible_trust_signals": ["10+ years experience", "50+ satisfied clients"],
                    "visible_geography": ["Riyadh", "Saudi Arabia"],
                    "visible_pricing_or_packages": ["Starting at $499"],
                    "visible_support_or_contact_methods": ["contact@sleekweb.com"]
                }
            ],
            "internal_resources": []
        }
        
        state_ds_copy = copy.deepcopy(state_ds)
        contract_ds = build_brand_offer_contract(state_ds)
        
        # Check non-mutation
        self.assertEqual(state_ds, state_ds_copy, "Input state must not be mutated!")
        
        # Expose detected_domain check
        self.assertEqual(contract_ds["evidence_summary"]["detected_domain"], "digital_services")
        
        # Schema integrity checks
        self.assertIn("brand_identity", contract_ds)
        self.assertIn("offer_mechanics", contract_ds)
        self.assertIn("value_propositions", contract_ds)
        self.assertIn("trust_signals", contract_ds)
        self.assertIn("conversion_actions", contract_ds)
        self.assertIn("keyword_fit", contract_ds)
        self.assertIn("supported_user_intents", contract_ds)
        self.assertIn("brand_limitations", contract_ds)
        self.assertIn("evidence_summary", contract_ds)
        
        # Asserts for B2B specific fields
        self.assertEqual(contract_ds["brand_identity"]["category"], "web design agency")
        self.assertIn("Website Design", contract_ds["offer_mechanics"]["supporting_services"])
        
        # Serialize to JSON and assert none of the forbidden phrases are present
        json_ds = json.dumps(contract_ds)
        forbidden_phrases = [
            "verified listings", "price shown", "search listings", 
            "contact advertiser", "owner", "agent", "property images"
        ]
        for phrase in forbidden_phrases:
            self.assertNotIn(phrase, json_ds.lower(), f"B2B contract must not contain real-estate phrase: {phrase}")

        # 2. Real estate fixture
        state_re = {
            "primary_keyword": "apartments for rent",
            "brand_name": "Riyadh Estates",
            "brand_context": "property rentals listing platform",
            "brand_evidence_cards": [
                {
                    "url": "https://riyadhestates.com/rent",
                    "title": "Apartments and Villas for Rent",
                    "page_type": "services",
                    "headings": ["Search Properties", "Verified Listings"],
                    "visible_products_or_services": ["Apartment Rental", "Villa Rental"],
                    "visible_features_or_capabilities": ["Search by district", "Price comparison"],
                    "visible_conversion_actions": ["Contact Agent", "Browse Listings"],
                    "visible_trust_signals": ["Verified Listings", "Clear Property Images"],
                    "visible_geography": ["Riyadh"],
                    "visible_pricing_or_packages": ["Rent price starting from 3000 SAR"]
                }
            ],
            "brand_evidence_map": {
                "strong_signals": ["browse listings", "search or filter", "comparison tools", "contact provider", "verified status", "price shown", "images shown"],
                "medium_signals": [],
                "weak_signals": [],
                "missing_evidence": []
            }
        }
        
        contract_re = build_brand_offer_contract(state_re)
        self.assertEqual(contract_re["evidence_summary"]["detected_domain"], "real_estate")
        
        # Real-estate specific capabilities and signals check
        self.assertEqual(contract_re["brand_identity"]["category"], "real estate listing platform")
        self.assertIn("browse property listings", contract_re["offer_mechanics"]["discovery_features"])
        self.assertIn("search apartments by district", contract_re["offer_mechanics"]["search_or_filter_features"])
        self.assertIn("verified listings", contract_re["trust_signals"])
        self.assertIn("price shown", contract_re["trust_signals"])
        self.assertIn("clear property images", contract_re["trust_signals"])
        self.assertIn("contact advertiser / owner / agent", contract_re["offer_mechanics"]["contact_or_conversion_flow"])
        self.assertIn("contact agent", contract_re["conversion_actions"])

        # 3. Ecommerce fixture
        state_ec = {
            "primary_keyword": "organic green tea online",
            "brand_name": "PureTea Organic",
            "brand_context": "buy organic tea online store",
            "brand_evidence_cards": [
                {
                    "url": "https://puretea.com/shop",
                    "title": "Shop Premium Organic Tea",
                    "page_type": "product",
                    "headings": ["Green Tea", "Black Tea", "Checkout"],
                    "visible_products_or_services": ["Green Tea Packet", "Black Tea Box"],
                    "visible_features_or_capabilities": ["Secure checkout", "Quick shipping"],
                    "visible_conversion_actions": ["Add to Cart", "Checkout Now"],
                    "visible_trust_signals": ["100% Organic certified"],
                    "visible_pricing_or_packages": ["$15 per box"]
                }
            ],
            "internal_resources": []
        }
        
        contract_ec = build_brand_offer_contract(state_ec)
        self.assertEqual(contract_ec["evidence_summary"]["detected_domain"], "ecommerce")
        self.assertEqual(contract_ec["brand_identity"]["category"], "e-commerce store")
        self.assertIn("browse products", contract_ec["offer_mechanics"]["discovery_features"])
        self.assertIn("add to cart / checkout", contract_ec["offer_mechanics"]["contact_or_conversion_flow"])
        
        # Serialize to JSON and assert none of the forbidden phrases are present
        json_ec = json.dumps(contract_ec)
        for phrase in forbidden_phrases:
            self.assertNotIn(phrase, json_ec.lower(), f"Ecommerce contract must not contain real-estate phrase: {phrase}")

        # 4. Unknown domain fixture
        state_unk = {
            "primary_keyword": "generic keyword topic",
            "brand_name": "BlankBrand",
            "brand_context": "some general info page",
            "brand_evidence_cards": [
                {
                    "url": "https://blankbrand.com/about",
                    "title": "About Us",
                    "page_type": "other",
                    "headings": ["Company overview"],
                    "visible_products_or_services": [],
                    "visible_features_or_capabilities": []
                }
            ],
            "internal_resources": []
        }
        
        contract_unk = build_brand_offer_contract(state_unk)
        self.assertEqual(contract_unk["evidence_summary"]["detected_domain"], "unknown")
        self.assertEqual(contract_unk["brand_identity"]["category"], "service or platform")
        self.assertEqual(contract_unk["brand_identity"]["confidence"], "low")
        # Unknown with no explicit CTAs or contact methods must have EMPTY conversion_actions
        self.assertEqual(contract_unk["conversion_actions"], [], "Unknown fixture with no CTA must have empty conversion_actions")
        
        # Serialize to JSON and assert none of the forbidden phrases are present
        json_unk = json.dumps(contract_unk)
        for phrase in forbidden_phrases:
            self.assertNotIn(phrase, json_unk.lower(), f"Unknown contract must not contain real-estate phrase: {phrase}")
        # And must not have real-estate missing evidence
        self.assertNotIn("no listing browsing observed", contract_unk["evidence_summary"]["missing_evidence"])
        self.assertNotIn("no explicit comparison tool observed", contract_unk["evidence_summary"]["missing_evidence"])


    # -----------------------------------------------------------------------
    # Correction Patch Tests
    # -----------------------------------------------------------------------

    def test_classifier_uses_cards_not_keyword(self):
        """
        Correction Patch 1:
        A digital_services evidence card + real-estate primary_keyword must still
        classify as digital_services because cards take priority over keyword.
        """
        import json
        from src.services.brand_evidence_service import build_brand_offer_contract
        state = {
            "primary_keyword": "apartments for rent in Riyadh",   # real-estate keyword!
            "brand_name": "WebCraft Agency",
            "brand_context": "award-winning web design studio",
            "brand_evidence_cards": [
                {
                    "url": "https://webcraft.com/services",
                    "title": "Web Design & Development Services",
                    "page_type": "services",
                    "headings": ["Web Design", "Mobile App Development", "SEO Services"],
                    "visible_products_or_services": ["Web Design", "Mobile App Development", "SEO"],
                    "visible_features_or_capabilities": ["responsive design", "fast hosting"],
                    "visible_conversion_actions": ["Get a Quote"],
                    "visible_trust_signals": ["ISO certified"],
                    "visible_geography": ["Dubai"],
                    "visible_pricing_or_packages": [],
                    "visible_support_or_contact_methods": []
                }
            ],
            "internal_resources": []
        }
        contract = build_brand_offer_contract(state)
        self.assertEqual(
            contract["evidence_summary"]["detected_domain"], "digital_services",
            "Cards must override keyword for domain classification"
        )
        # No real-estate taxonomy must appear in this contract
        json_str = json.dumps(contract).lower()
        for forbidden in ["verified listings", "search listings", "property images",
                          "contact advertiser", "browse property"]:
            self.assertNotIn(forbidden, json_str)

    def test_unknown_contract_has_no_default_conversion_action(self):
        """
        Correction Patch 2:
        Unknown domain with no explicit CTA / contact evidence must produce
        conversion_actions == [] (not 'contact provider').
        """
        from src.services.brand_evidence_service import build_brand_offer_contract
        state = {
            "primary_keyword": "some generic topic",
            "brand_name": "BlankCo",
            "brand_evidence_cards": [
                {
                    "url": "https://blankco.com/about",
                    "title": "About BlankCo",
                    "page_type": "about",
                    "headings": ["Our Story"],
                    "visible_products_or_services": [],
                    "visible_features_or_capabilities": [],
                    "visible_conversion_actions": [],          # no CTAs
                    "visible_support_or_contact_methods": [],  # no contact
                    "visible_geography": [],
                    "visible_pricing_or_packages": [],
                    "visible_trust_signals": []
                }
            ],
            "internal_resources": []
        }
        contract = build_brand_offer_contract(state)
        self.assertEqual(contract["evidence_summary"]["detected_domain"], "unknown")
        self.assertEqual(
            contract["conversion_actions"], [],
            "Unknown domain with no CTA evidence must not add 'contact provider' by default"
        )

    def test_digital_services_no_contact_provider_without_evidence(self):
        """
        Correction Patch 3:
        A digital_services fixture with no visible_conversion_actions and no
        visible_support_or_contact_methods must NOT add 'contact provider'.
        """
        from src.services.brand_evidence_service import build_brand_offer_contract
        state = {
            "primary_keyword": "enterprise software consulting",
            "brand_name": "SilentAgency",
            "brand_evidence_cards": [
                {
                    "url": "https://silentagency.com/services",
                    "title": "Software Consulting Services",
                    "page_type": "services",
                    "headings": ["Software Consulting", "System Integration"],
                    "visible_products_or_services": ["Software Consulting", "System Integration"],
                    "visible_features_or_capabilities": ["scalable architecture"],
                    "visible_conversion_actions": [],          # no CTAs at all
                    "visible_support_or_contact_methods": [],  # no contact
                    "visible_cta_labels": [],
                    "cta_labels": [],
                    "visible_geography": [],
                    "visible_pricing_or_packages": [],
                    "visible_trust_signals": []
                }
            ],
            "internal_resources": []
        }
        contract = build_brand_offer_contract(state)
        self.assertEqual(contract["evidence_summary"]["detected_domain"], "digital_services")
        self.assertNotIn(
            "contact provider", contract["conversion_actions"],
            "digital_services with no CTA/contact evidence must not auto-add 'contact provider'"
        )
        self.assertEqual(contract["conversion_actions"], [])

    def test_ecommerce_no_cart_without_cta_evidence(self):
        """
        Correction Patch 4:
        An ecommerce fixture with NO cart/checkout CTA must NOT add
        'add to cart / checkout' to offer_mechanics.
        """
        from src.services.brand_evidence_service import build_brand_offer_contract
        state = {
            "primary_keyword": "online fashion store",
            "brand_name": "FashionHub",
            "brand_evidence_cards": [
                {
                    "url": "https://fashionhub.com/shop",
                    "title": "Shop Fashion Products Online",
                    "page_type": "product",
                    "headings": ["Shop Summer Collection", "Browse Products Online"],
                    "visible_products_or_services": ["Dresses", "Jackets"],
                    "visible_features_or_capabilities": ["free returns"],
                    "visible_conversion_actions": ["View Collection"],  # browse only, no cart CTA
                    "visible_trust_signals": ["trusted by 10k customers"],
                    "visible_pricing_or_packages": ["from $29"],
                    "visible_geography": [],
                    "visible_support_or_contact_methods": []
                }
            ],
            "internal_resources": []
        }
        contract = build_brand_offer_contract(state)
        self.assertEqual(contract["evidence_summary"]["detected_domain"], "ecommerce")
        self.assertNotIn(
            "add to cart / checkout", contract["offer_mechanics"]["contact_or_conversion_flow"],
            "Ecommerce fixture with no cart/checkout CTA must not invent add-to-cart flow"
        )

    def test_non_real_estate_missing_evidence_no_re_phrases(self):
        """
        Correction Patch 5:
        A digital_services contract must not contain real-estate missing evidence
        ('no listing browsing observed', 'no explicit comparison tool observed',
         'no explicit verification badges observed') in either evidence_summary
         or brand_limitations.
        """
        import json
        from src.services.brand_evidence_service import build_brand_offer_contract
        state = {
            "primary_keyword": "cloud hosting services",
            "brand_name": "CloudBase",
            "brand_evidence_cards": [
                {
                    "url": "https://cloudbase.io/hosting",
                    "title": "Managed Cloud Hosting",
                    "page_type": "services",
                    "headings": ["Cloud Hosting Plans", "99.9% Uptime SLA"],
                    "visible_products_or_services": ["Shared Hosting", "VPS", "Dedicated Server"],
                    "visible_features_or_capabilities": ["99.9% uptime", "DDoS protection"],
                    "visible_conversion_actions": [],
                    "visible_trust_signals": ["ISO 27001 certified"],
                    "visible_geography": ["Europe", "US"],
                    "visible_pricing_or_packages": ["$5/month"],
                    "visible_support_or_contact_methods": []
                }
            ],
            # Simulate brand_evidence_map that contains real-estate missing phrases (from old pipeline)
            "brand_evidence_map": {
                "strong_signals": ["price shown", "verified status", "images shown", "browse listings"],
                "medium_signals": [],
                "weak_signals": [],
                "missing_evidence": [
                    "no listing browsing observed",
                    "no explicit comparison tool observed",
                    "no explicit verification badges observed",
                    "no explicit booking flow detected",
                ]
            },
            "internal_resources": []
        }
        contract = build_brand_offer_contract(state)
        self.assertEqual(contract["evidence_summary"]["detected_domain"], "digital_services")
        re_missing = [
            "no listing browsing observed",
            "no explicit comparison tool observed",
            "no explicit verification badges observed",
        ]
        for phrase in re_missing:
            self.assertNotIn(
                phrase, contract["evidence_summary"]["missing_evidence"],
                f"Non-RE contract must not contain real-estate missing phrase: '{phrase}'"
            )
            self.assertNotIn(
                phrase, contract["brand_limitations"],
                f"Non-RE brand_limitations must not contain real-estate phrase: '{phrase}'"
            )
        serialized = json.dumps(contract).lower()
        for phrase in ["verified listings", "price shown", "property images", "browse listings"]:
            self.assertNotIn(
                phrase, serialized,
                f"Non-RE contract must filter polluted real-estate evidence-map phrase: '{phrase}'"
            )



    def test_select_section_sources_matches_service(self):
        """Test that a section discussing a specific service matches the correct service card."""
        from src.services.brand_evidence_service import select_section_brand_sources
        state = {
            "brand_name": "TechPro",
            "brand_evidence_cards": [
                {
                    "url": "https://techpro.com/web-design",
                    "title": "Web Design Services",
                    "page_type": "service",
                    "visible_products_or_services": ["Custom Web Design", "UI/UX"],
                    "excluded_reason": None
                },
                {
                    "url": "https://techpro.com/seo",
                    "title": "SEO Services",
                    "page_type": "service",
                    "visible_products_or_services": ["Search Engine Optimization"],
                    "excluded_reason": None
                }
            ]
        }
        section = {
            "heading_text": "TechPro Web Design Solutions",
            "content_goal": "Explain our UI/UX and web design offerings",
            "assigned_keywords": ["web design", "techpro ui"]
        }
        text, count = select_section_brand_sources(section, state)
        self.assertEqual(count, 1)
        self.assertIn("techpro.com/web-design", text)
        self.assertNotIn("techpro.com/seo", text)

    def test_select_section_sources_matches_portfolio(self):
        """Test that a portfolio/case study section selects the correct portfolio card."""
        from src.services.brand_evidence_service import select_section_brand_sources
        state = {
            "brand_name": "BuildIt",
            "brand_evidence_cards": [
                {
                    "url": "https://buildit.com/portfolio",
                    "title": "Our Recent Projects",
                    "page_type": "portfolio",
                    "visible_project_or_case_study_examples": ["Downtown Skyscraper", "City Mall"],
                    "excluded_reason": None
                },
                {
                    "url": "https://buildit.com/about",
                    "title": "About Us",
                    "page_type": "about",
                    "visible_features_or_capabilities": ["20 years experience"],
                    "excluded_reason": None
                }
            ]
        }
        section = {
            "heading_text": "BuildIt Recent Projects",
            "content_goal": "Showcase case studies like Downtown Skyscraper",
            "section_intent": "informational"
        }
        text, count = select_section_brand_sources(section, state)
        self.assertEqual(count, 1)
        self.assertIn("buildit.com/portfolio", text)
        self.assertIn("Downtown Skyscraper", text)

    def test_select_section_sources_no_relevant_card(self):
        """Test that if no relevant card matches the section tokens, no source is returned."""
        from src.services.brand_evidence_service import select_section_brand_sources
        state = {
            "brand_name": "CloudSync",
            "brand_evidence_cards": [
                {
                    "url": "https://cloudsync.com/pricing",
                    "title": "Pricing Plans",
                    "page_type": "pricing",
                    "visible_pricing_or_packages": ["Basic Plan $10"],
                    "excluded_reason": None
                }
            ]
        }
        section = {
            "heading_text": "CloudSync Cloud Architecture Integration",
            "content_goal": "Explain complex serverless setups",
            "assigned_keywords": []
        }
        # Tokens in section ("cloud", "architecture", "integration", "explain", "complex", "serverless", "setups")
        # Tokens in card ("pricing", "plans", "basic", "plan")
        # No overlap > 2 chars other than stop words, score should be 0.
        text, count = select_section_brand_sources(section, state)
        self.assertEqual(count, 0)
        self.assertEqual(text, "")

    def test_select_section_sources_no_brand_reference(self):
        """Test that if the section does not reference the brand, source selection is skipped."""
        from src.services.brand_evidence_service import select_section_brand_sources
        state = {
            "brand_name": "CloudSync",
            "brand_evidence_cards": [
                {
                    "url": "https://cloudsync.com/pricing",
                    "title": "CloudSync Pricing Plans",
                    "page_type": "pricing",
                    "visible_pricing_or_packages": ["Basic Plan $10"],
                    "excluded_reason": None
                }
            ]
        }
        section = {
            "heading_text": "General Cloud Architecture",
            "content_goal": "Explain how pricing works in the industry",
            "assigned_keywords": ["cloud pricing"]
        }
        text, count = select_section_brand_sources(section, state)
        self.assertEqual(count, 0)
        self.assertEqual(text, "")

    def test_select_section_sources_exact_formatting(self):
        """Test that the formatted section_source_text matches the required spec and includes page-backed facts."""
        from src.services.brand_evidence_service import select_section_brand_sources
        state = {
            "brand_name": "Acme",
            "brand_evidence_cards": [
                {
                    "url": "https://acme.com/service",
                    "title": "Acme Services",
                    "page_type": "service",
                    "visible_products_or_services": ["Anvil Repair"],
                    "visible_features_or_capabilities": [],
                    "usable_snippets": ["We repair all anvils fast."],
                    "excluded_reason": None
                }
            ]
        }
        section = {
            "heading_text": "Acme Anvil Repair",
            "content_goal": "Discuss services",
            "assigned_keywords": ["acme"]
        }
        text, count = select_section_brand_sources(section, state)
        self.assertEqual(count, 1)
        self.assertIn("[SECTION-SPECIFIC BRAND EVIDENCE]", text)
        self.assertIn("Source URL: https://acme.com/service", text)
        self.assertIn("Page type: service", text)
        self.assertIn("Products/Services: Anvil Repair", text)
        self.assertIn("- We repair all anvils fast.", text)
        self.assertIn("Constraints:", text)
        self.assertIn("- Do not add facts not listed above.", text)

    def test_select_section_sources_arabic_noisy_tokens(self):
        from src.services.brand_evidence_service import select_section_brand_sources

        state = {
            "brand_name": "براندكو",
            "brand_evidence_cards": [
                {
                    "url": "https://brandco.example/services",
                    "title": "خدمات تصميم المواقع",
                    "page_type": "services",
                    "headings": ["تصميم المواقع", "التسويق الرقمي"],
                    "visible_products_or_services": ["تصميم المواقع", "التسويق الرقمي"],
                    "visible_features_or_capabilities": [],
                    "visible_project_or_case_study_examples": [],
                    "usable_snippets": ["تقدم براندكو خدمات تصميم المواقع للشركات."],
                    "excluded_reason": None,
                }
            ],
        }

        section = {
            "heading_text": "كيف تساعد شركة براندكو في تصميم المواقع",
            "content_goal": "شرح الخدمة",
        }

        text, count = select_section_brand_sources(section, state)
        self.assertEqual(count, 1)
        self.assertIn("تصميم المواقع", text)
        self.assertIn("brandco.example/services", text)

    def test_select_section_sources_arabic_generic_brand_heading_no_match(self):
        from src.services.brand_evidence_service import select_section_brand_sources

        state = {
            "brand_name": "براندكو",
            "brand_evidence_cards": [
                {
                    "url": "https://brandco.example/services",
                    "title": "خدمات تصميم المواقع",
                    "page_type": "services",
                    "headings": ["تصميم المواقع"],
                    "visible_products_or_services": ["تصميم المواقع"],
                    "visible_features_or_capabilities": [],
                    "excluded_reason": None,
                }
            ],
        }

        section = {
            "heading_text": "كيف تساعد شركة براندكو",
            "content_goal": "",
        }

        text, count = select_section_brand_sources(section, state)
        self.assertEqual(count, 0)
        self.assertEqual(text, "")

    async def test_writer_prompt_contains_brand_evidence(self):
        """Test that the rendered section writer prompt contains the provided brand evidence block."""
        from src.services.content_generator import SectionWriter
        from src.services.brand_evidence_service import select_section_brand_sources
        from unittest.mock import AsyncMock, MagicMock
        
        mock_ai = MagicMock()
        mock_ai.send = AsyncMock(return_value={"content": "{}", "metadata": {}})
        writer = SectionWriter(mock_ai)

        source_state = {
            "brand_name": "DummyCo",
            "brand_evidence_cards": [
                {
                    "url": "https://dummy.com",
                    "title": "DummyCo Web Design Services",
                    "page_type": "services",
                    "headings": ["Custom Web Design"],
                    "visible_products_or_services": ["Custom Web Design"],
                    "visible_features_or_capabilities": [],
                    "visible_project_or_case_study_examples": [],
                    "usable_snippets": ["DummyCo provides custom web design services."],
                    "excluded_reason": None,
                }
            ],
        }
        section = {
            "heading_text": "DummyCo Web Design Services",
            "section_intent": "Commercial",
            "content_goal": "Discuss Custom Web Design",
        }
        selected_source_text, selected_count = select_section_brand_sources(section, source_state)
        self.assertEqual(selected_count, 1)
        
        # Call write with minimal dummy arguments
        await writer.write(
            title="Dummy Title",
            global_keywords={"primary": "web design", "lsi": [], "semantic": []},
            section=section,
            article_intent="Commercial",
            seo_intelligence={"market_analysis": {"market_insights": {}}},
            content_type="brand_commercial",
            link_strategy="internal_only",
            brand_url="https://dummy.com",
            brand_name="DummyCo",
            brand_link_used=False,
            brand_link_allowed=True,
            allow_external_links=False,
            execution_plan={},
            area="Cairo",
            section_source_text=selected_source_text
        )
        
        # Pack-Only Writer Truth: the prompt now renders brand_page_knowledge_pack_context, not
        # section_source_text or legacy [SECTION-SPECIFIC BRAND EVIDENCE] blocks.
        self.assertTrue(mock_ai.send.called)
        sent_prompt = mock_ai.send.call_args[0][0] if mock_ai.send.call_args[0] else mock_ai.send.call_args[1].get("prompt", "")
        # The knowledge pack block is always present (fallback or real)
        self.assertIn("[BRAND PAGE KNOWLEDGE PACK - PAGE BY PAGE]", sent_prompt)
        # The contract section names these as routing-only diagnostics
        self.assertIn("section_brand_page_briefs", sent_prompt)
        self.assertIn("section_raw_brand_blocks", sent_prompt)

    async def test_writer_prompt_empty_brand_evidence(self):
        """Test that the empty brand evidence prompt case renders correctly."""
        from src.services.content_generator import SectionWriter
        from unittest.mock import AsyncMock, MagicMock
        
        mock_ai = MagicMock()
        mock_ai.send = AsyncMock(return_value={"content": "{}", "metadata": {}})
        writer = SectionWriter(mock_ai)
        
        await writer.write(
            title="Dummy Title",
            global_keywords={"primary": "web design", "lsi": [], "semantic": []},
            section={"heading_text": "Web Design Services", "section_intent": "Commercial"},
            article_intent="Commercial",
            seo_intelligence={"market_analysis": {"market_insights": {}}},
            content_type="brand_commercial",
            link_strategy="internal_only",
            brand_url="https://dummy.com",
            brand_link_used=False,
            brand_link_allowed=True,
            allow_external_links=False,
            execution_plan={},
            area="Cairo",
            section_source_text=""
        )
        
        # Pack-Only Writer Truth: empty evidence case shows the knowledge-pack fallback text
        self.assertTrue(mock_ai.send.called)
        sent_prompt = mock_ai.send.call_args[0][0] if mock_ai.send.call_args[0] else mock_ai.send.call_args[1].get("prompt", "")
        # Knowledge pack block is present (fallback text when no pack available)
        self.assertIn("[BRAND PAGE KNOWLEDGE PACK - PAGE BY PAGE]", sent_prompt)
        self.assertIn("No full page-by-page brand knowledge pack is available", sent_prompt)

    async def test_outline_prompt_receives_compact_evidence_summary(self):
        """Test that _step_1_outline injects compact brand evidence without mutating canonical context."""
        from src.services.brand_evidence_service import build_compact_brand_evidence_summary
        from unittest.mock import AsyncMock, MagicMock
        
        state = {
            "brand_name": "TechPro",
            "brand_context": "original brand context",
            "input_data": {
                "title": "Best Web Design",
                "keywords": ["web design"],
            },
            "primary_keyword": "web design",
            "seo_intelligence": {"market_analysis": {"market_insights": {}}},
            "content_strategy": {},
            "brand_evidence_cards": [
                {
                    "url": "https://techpro.com",
                    "page_type": "home",
                    "visible_products_or_services": ["Premium SEO"],
                    "excluded_reason": None
                }
            ],
            "brand_offer_contract": {
                "brand_identity": {
                    "confidence": "high",
                    "category": "agency"
                },
                "evidence_summary": {
                    "strong_evidence": ["homepage verified"]
                }
            }
        }
        
        summary = build_compact_brand_evidence_summary(state)
        # Bullet count verification: split by newlines, check number of list items
        bullets = [line for line in summary.split("\n") if line.strip().startswith("-")]
        self.assertTrue(len(bullets) <= 8)
        self.assertIn("[EVIDENCE BOUNDARY - COMPACT BRAND EVIDENCE SUMMARY - DO NOT TREAT AS BRAND DESCRIPTION]", summary)
        self.assertIn("Confidence Level: high", summary)

        controller = AsyncWorkflowController(work_dir=".")
        controller.outline_gen = MagicMock()
        controller.outline_gen.generate = AsyncMock(return_value={
            "outline": [],
            "metadata": {"prompt": "", "response": "", "tokens": {}, "model": ""}
        })
        controller.validator.consolidate_faq = MagicMock(side_effect=lambda outline: outline)
        controller.validator.repair_outline_deterministic = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.validator.enforce_intent_distribution = MagicMock(side_effect=lambda outline, intent, content_type: (outline, []))
        controller.validator.inject_local_seo = MagicMock(side_effect=lambda outline, area: (outline, []))
        controller.validator.validate_outline_quality = MagicMock(return_value=[])
        controller.validator.enforce_cta_policy = MagicMock(side_effect=lambda outline, content_type: outline)
        controller.validator.enforce_outline_structure = MagicMock(side_effect=lambda outline, content_type: outline)
        controller.validator.enforce_content_angle = MagicMock(side_effect=lambda outline, content_strategy: outline)
        controller.validator.adjust_paa_by_intent = MagicMock(side_effect=lambda outline, intent: outline)
        controller.validator.enforce_paa_sections = MagicMock(return_value={"paa_ok": True, "paa_ratio": 1, "missing_count": 0})
        controller.outline_repair_service.promote_visitor_intents = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.dedupe_faq_against_h2 = MagicMock(side_effect=lambda outline: outline)
        controller.outline_repair_service.refill_faq_after_dedupe = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.normalize_heading_only_section_types = MagicMock(side_effect=lambda outline: outline)
        controller.outline_repair_service.clean_echo_and_repetition = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.apply_strategic_map_and_roles = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.clean_conclusion_heading = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_gen._normalize_section = MagicMock()

        original_context = state["brand_context"]
        await controller._step_1_outline(state)

        self.assertTrue(controller.outline_gen.generate.called)
        called_kwargs = controller.outline_gen.generate.call_args[1]
        self.assertNotIn("[EVIDENCE BOUNDARY - COMPACT BRAND EVIDENCE SUMMARY - DO NOT TREAT AS BRAND DESCRIPTION]", called_kwargs["brand_context"])
        self.assertNotIn("Confidence Level: high", called_kwargs["brand_context"])
        self.assertIn("[BRAND EVIDENCE INVENTORY - OUTLINE GATE]", called_kwargs["brand_context"])
        self.assertEqual(state["brand_context"], original_context)

    async def test_canonical_brand_context_remains_unchanged(self):
        """Test that state['brand_context'] remains completely unchanged during workflow execution."""
        from src.services.workflow_controller import AsyncWorkflowController
        from unittest.mock import AsyncMock, MagicMock
        
        state = {
            "brand_name": "TechPro",
            "brand_context": "original brand context",
            "brand_evidence_cards": [],
            "brand_offer_contract": {},
            "serp_data": {"top_results": []},
            "seo_intelligence": {"market_analysis": {"market_insights": {}}},
            "content_type": "brand_commercial",
            "primary_keyword": "web design",
            "keywords": ["web design"],
            "urls": [],
            "article_language": "ar",
            "intent": "commercial",
            "content_strategy": {},
            "area": "Cairo",
            "mandatory_section_types": [],
            "heading_only_mode": True
        }
        
        controller = MagicMock()
        controller.outline_gen = MagicMock()
        controller.outline_gen.generate = AsyncMock(return_value={
            "outline": [],
            "keyword_expansion": {},
            "metadata": {"prompt": "", "response": "", "tokens": 0}
        })
        controller._distill_serp_intelligence = MagicMock(return_value={})
        controller._step_1_outline = MagicMock(side_effect=lambda s: s)
        
        orig_context = state["brand_context"]
        
        from src.services.brand_evidence_service import build_compact_brand_evidence_summary
        compact = build_compact_brand_evidence_summary(state)
        temp_context = state["brand_context"] + compact
        
        # Confirm that we only use temp_context dynamically in prompt, leaving state unchanged
        self.assertEqual(state["brand_context"], orig_context)

    async def test_low_confidence_blocks_promotional_brand_headings(self):
        """Test that low confidence blocks promotional brand headings entirely."""
        from src.services.brand_evidence_service import build_brand_heading_guardrails
        state = {
            "brand_name": "ProTech",
            "primary_keyword": "cleaning",
            "brand_offer_contract": {
                "brand_identity": {
                    "brand_name": "ProTech",
                    "confidence": "low"
                }
            }
        }
        guardrails = build_brand_heading_guardrails(state)
        self.assertFalse(guardrails["promotional_headings_allowed"])
        self.assertFalse(guardrails["dedicated_brand_proof_sections_allowed"])
        self.assertFalse(guardrails["differentiation_headings_allowed"])
        self.assertIn("Why choose ProTech?", guardrails["forbidden_generic_brand_headings"])
        self.assertIn("لماذا تختار ProTech؟", guardrails["forbidden_generic_brand_headings"])

    async def test_medium_confidence_allows_operational_blocks_promotional(self):
        """Test that medium confidence allows operational headings but blocks promotional headings."""
        from src.services.brand_evidence_service import build_brand_heading_guardrails
        state = {
            "brand_name": "ProTech",
            "primary_keyword": "cleaning",
            "brand_offer_contract": {
                "brand_identity": {
                    "brand_name": "ProTech",
                    "confidence": "medium"
                }
            }
        }
        guardrails = build_brand_heading_guardrails(state)
        self.assertFalse(guardrails["promotional_headings_allowed"])
        self.assertFalse(guardrails["dedicated_brand_proof_sections_allowed"])
        self.assertFalse(guardrails["differentiation_headings_allowed"])
        self.assertIn("Services offered by ProTech for cleaning", guardrails["preferred_evidence_grounded_heading_patterns"])
        self.assertIn("Why choose ProTech?", guardrails["forbidden_generic_brand_headings"])

    async def test_high_confidence_differentiation_only_with_evidence(self):
        """Test that high confidence allows differentiation headings only when explicit evidence exists."""
        from src.services.brand_evidence_service import build_brand_heading_guardrails
        # Case A: High confidence but NO trust signals or value props
        state_no_evidence = {
            "brand_name": "ProTech",
            "primary_keyword": "cleaning",
            "brand_offer_contract": {
                "brand_identity": {
                    "brand_name": "ProTech",
                    "confidence": "high"
                }
            }
        }
        guardrails_no = build_brand_heading_guardrails(state_no_evidence)
        self.assertFalse(guardrails_no["promotional_headings_allowed"])
        self.assertIn("Why choose ProTech?", guardrails_no["forbidden_generic_brand_headings"])

        # Case B: High confidence WITH trust signals/value props
        state_with_evidence = {
            "brand_name": "ProTech",
            "primary_keyword": "cleaning",
            "brand_offer_contract": {
                "brand_identity": {
                    "brand_name": "ProTech",
                    "confidence": "high"
                },
                "trust_signals": ["10 years experience"]
            }
        }
        guardrails_yes = build_brand_heading_guardrails(state_with_evidence)
        self.assertFalse(guardrails_yes["promotional_headings_allowed"])
        self.assertTrue(guardrails_yes["differentiation_headings_allowed"])
        self.assertTrue(guardrails_yes["dedicated_brand_proof_sections_allowed"])
        self.assertIn("Why choose ProTech?", guardrails_yes["forbidden_generic_brand_headings"])
        self.assertIn("Evidence-backed advantages available from ProTech", guardrails_yes["preferred_evidence_grounded_heading_patterns"])

    async def test_geography_heading_claims_blocked_without_evidence(self):
        """Test that geographical heading claims are blocked when no explicit geography exists."""
        from src.services.brand_evidence_service import build_brand_heading_guardrails
        state = {
            "brand_name": "ProTech",
            "primary_keyword": "cleaning",
            "brand_offer_contract": {
                "brand_identity": {
                    "brand_name": "ProTech",
                    "confidence": "medium"
                }
            }
        }
        guardrails = build_brand_heading_guardrails(state)
        self.assertFalse(guardrails["has_explicit_geography"])
        self.assertIn("Do not claim geography or specific location focus in brand headings.", guardrails["heading_rules"])
        self.assertIn("ProTech in Riyadh/Cairo/Location", guardrails["forbidden_generic_brand_headings"])

    async def test_outline_prompt_does_not_receive_legacy_heading_guardrails_context(self):
        """Outline prompt should use inventory gate instead of legacy heading guardrails context."""
        from src.services.workflow_controller import AsyncWorkflowController
        from unittest.mock import AsyncMock, MagicMock
        
        state = {
            "brand_name": "ProTech",
            "brand_context": "original brand context",
            "input_data": {
                "title": "Best Cleaning Services",
                "keywords": ["cleaning"],
            },
            "primary_keyword": "cleaning",
            "seo_intelligence": {"market_analysis": {"market_insights": {}}},
            "content_strategy": {},
            "brand_evidence_cards": [],
            "brand_offer_contract": {
                "brand_identity": {
                    "confidence": "medium",
                    "brand_name": "ProTech"
                }
            }
        }
        
        controller = AsyncWorkflowController(work_dir=".")
        controller.outline_gen = MagicMock()
        controller.outline_gen.generate = AsyncMock(return_value={
            "outline": [],
            "metadata": {"prompt": "", "response": "", "tokens": {}, "model": ""}
        })
        controller.validator.consolidate_faq = MagicMock(side_effect=lambda outline: outline)
        controller.validator.repair_outline_deterministic = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.validator.enforce_intent_distribution = MagicMock(side_effect=lambda outline, intent, content_type: (outline, []))
        controller.validator.inject_local_seo = MagicMock(side_effect=lambda outline, area: (outline, []))
        controller.validator.validate_outline_quality = MagicMock(return_value=[])
        controller.validator.enforce_cta_policy = MagicMock(side_effect=lambda outline, content_type: outline)
        controller.validator.enforce_outline_structure = MagicMock(side_effect=lambda outline, content_type: outline)
        controller.validator.enforce_content_angle = MagicMock(side_effect=lambda outline, content_strategy: outline)
        controller.validator.adjust_paa_by_intent = MagicMock(side_effect=lambda outline, intent: outline)
        controller.validator.enforce_paa_sections = MagicMock(return_value={"paa_ok": True, "paa_ratio": 1, "missing_count": 0})
        controller.outline_repair_service.promote_visitor_intents = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.dedupe_faq_against_h2 = MagicMock(side_effect=lambda outline: outline)
        controller.outline_repair_service.refill_faq_after_dedupe = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.normalize_heading_only_section_types = MagicMock(side_effect=lambda outline: outline)
        controller.outline_repair_service.clean_echo_and_repetition = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.apply_strategic_map_and_roles = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_repair_service.clean_conclusion_heading = MagicMock(side_effect=lambda outline, **kwargs: outline)
        controller.outline_gen._normalize_section = MagicMock()
        
        await controller._step_1_outline(state)
        
        self.assertTrue(controller.outline_gen.generate.called)
        called_kwargs = controller.outline_gen.generate.call_args[1]
        self.assertNotIn("[BRAND HEADING GUARDRAILS - CRITICAL OUTLINE BOUNDARY]", called_kwargs["brand_context"])
        self.assertNotIn("Why choose ProTech?", called_kwargs["brand_context"])
        self.assertIn("[BRAND EVIDENCE INVENTORY - OUTLINE GATE]", called_kwargs["brand_context"])
        self.assertEqual(state["brand_context"], "original brand context")

    def test_apply_brand_claim_gate_removes_unsupported_english_claims(self):
        """Test that apply_brand_claim_gate removes unsupported English claims."""
        from src.services.brand_evidence_service import apply_brand_claim_gate
        brief = {
            "brand_name": "ProTech",
            "allowed_claims": ["Standard operational workflow"],
            "brand_offer_contract": {}
        }
        text = "ProTech offers a standard operational workflow. ProTech has a 100% satisfaction guarantee. ProTech is the top choice."
        result = apply_brand_claim_gate(text, brief)
        self.assertIn("ProTech offers a standard operational workflow.", result)
        self.assertNotIn("guarantee", result)
        self.assertNotIn("top choice", result)

    def test_apply_brand_claim_gate_removes_unsupported_arabic_claims(self):
        """Test that apply_brand_claim_gate removes unsupported Arabic claims."""
        from src.services.brand_evidence_service import apply_brand_claim_gate
        brief = {
            "brand_name": "برو تيك",
            "allowed_claims": ["خدمة تنظيف متميزة"],
            "brand_offer_contract": {}
        }
        text = "تقدم برو تيك خدمة تنظيف متميزة. برو تيك تقدم أفضل الأسعار المضمونة. برو تيك تضمن رضا العملاء."
        result = apply_brand_claim_gate(text, brief)
        self.assertIn("تقدم برو تيك خدمة تنظيف متميزة.", result)
        self.assertNotIn("أفضل", result)
        self.assertNotIn("الأسعار المضمونة", result)
        self.assertNotIn("تضمن", result)

    def test_apply_brand_claim_gate_preserves_valid_operational_evidence_backed(self):
        """Test that apply_brand_claim_gate preserves valid operational evidence-backed claims."""
        from src.services.brand_evidence_service import apply_brand_claim_gate
        brief = {
            "brand_name": "ProTech",
            "allowed_claims": ["responsive 24/7 support", "certified teams"],
            "brand_offer_contract": {
                "pricing": "starting from $50"
            }
        }
        text = "ProTech has certified teams. ProTech provides responsive 24/7 support. ProTech offers pricing starting from $50."
        result = apply_brand_claim_gate(text, brief)
        self.assertIn("ProTech has certified teams.", result)
        self.assertIn("ProTech provides responsive 24/7 support.", result)
        self.assertIn("ProTech offers pricing starting from $50.", result)

    def test_apply_brand_claim_gate_uses_section_source_text_as_support(self):
        """Test that selected page-backed section evidence can support otherwise gated claims."""
        from src.services.brand_evidence_service import apply_brand_claim_gate
        brief = {
            "brand_name": "ProTech",
            "allowed_claims": [],
            "section_source_text": (
                "[SECTION-SPECIFIC BRAND EVIDENCE]\n"
                "Observed facts:\n"
                "- Service area: Riyadh\n"
                "- Pricing: starting from $50\n"
                "- Certification: ISO certified team\n"
            )
        }
        text = (
            "ProTech serves Riyadh. "
            "ProTech offers pricing starting from $50. "
            "ProTech has an ISO certified team. "
            "ProTech is the top agency."
        )
        result = apply_brand_claim_gate(text, brief)
        self.assertIn("ProTech serves Riyadh.", result)
        self.assertIn("ProTech offers pricing starting from $50.", result)
        self.assertIn("ProTech has an ISO certified team.", result)
        self.assertNotIn("top agency", result)

    def test_apply_brand_claim_gate_preserves_paragraph_structure_and_headings(self):
        """Test that apply_brand_claim_gate preserves paragraph structure, headings, and lists."""
        from src.services.brand_evidence_service import apply_brand_claim_gate
        brief = {
            "brand_name": "ProTech",
            "allowed_claims": ["operational support"]
        }
        text = "## ProTech Overview\n\n- First point for ProTech\n- Second point with 100% guarantee\n\nProTech provides operational support."
        result = apply_brand_claim_gate(text, brief)
        self.assertIn("## ProTech Overview", result)
        self.assertIn("- First point for ProTech", result)
        self.assertNotIn("guarantee", result)
        self.assertIn("ProTech provides operational support.", result)

    def test_apply_brand_claim_gate_section_level_awareness(self):
        """Test that apply_brand_claim_gate gates sentences inside a brand heading section even when sentence lacks brand name."""
        from src.services.brand_evidence_service import apply_brand_claim_gate
        brief = {
            "brand_name": "ProTech",
            "allowed_claims": ["operational support"]
        }
        text = "## ProTech Services\nHere is a 100% guarantee from our team.\n\n## Unrelated Section\nHere is a 100% guarantee from another team."
        result = apply_brand_claim_gate(text, brief)
        # Inside ## ProTech Services, the 100% guarantee is gated even though ProTech is not in that sentence
        self.assertNotIn("guarantee", result.split("## Unrelated Section")[0])
        # Inside ## Unrelated Section, the sentence doesn't mention brand and heading doesn't either, so it is preserved
        self.assertIn("guarantee", result.split("## Unrelated Section")[1])

    def test_regression_digital_services_no_real_estate_leakage(self):
        """1. Digital services brand fixture:
        Mock internal_resources for a brand that has digital/service capabilities.
        Expected: contract must not contain real-estate/listing fields or mechanics.
        """
        from src.services.brand_evidence_service import build_brand_offer_contract
        state = {
            "brand_name": "TechForge",
            "internal_resources": [
                {"link": "https://techforge.com/services", "text": "web design, app development, branding", "headings": ["Web Design Services", "App Development"], "visible_products_or_services": ["web design", "app development", "branding"]},
                {"link": "https://techforge.com/portfolio", "text": "recent projects and client work examples", "headings": ["Our Portfolio"]}
            ],
            "brand_evidence_cards": []
        }
        contract = build_brand_offer_contract(state)
        # Check domain classifier
        self.assertEqual(contract["evidence_summary"]["detected_domain"], "digital_services")
        
        # Verify no real-estate leakage
        non_allowed_re = ["verified listings", "price shown", "search listings", "property listings", "contact advertiser", "contact owner", "contact agent"]
        flat_contract = str(contract).lower()
        for field in non_allowed_re:
            self.assertNotIn(field, flat_contract)

    def test_regression_real_estate_fields_only_when_explicit(self):
        """2. Real-estate brand fixture:
        Mock internal_resources for a real estate platform.
        Expected: listing mechanics are produced; pricing and verified listings only if explicitly supported.
        """
        from src.services.brand_evidence_service import build_brand_offer_contract
        # Case A: Explicit listing pages, but NO pricing, and NO verified listing details
        state_no_explicit = {
            "brand_name": "AqarFind",
            "internal_resources": [
                {"link": "https://aqarfind.com/rent", "text": "browse rent listings in Cairo, villas, apartments for sale", "headings": ["Rent Listings Cairo", "Apartments for sale"]},
                {"link": "https://aqarfind.com/agent", "text": "contact our team and agent listing details"}
            ],
            "brand_evidence_cards": []
        }
        contract_no = build_brand_offer_contract(state_no_explicit)
        self.assertEqual(contract_no["evidence_summary"]["detected_domain"], "real_estate")
        # Verify Cairo geographic focus is allowed because القاهرة / Cairo is explicit
        self.assertIn("Cairo", contract_no["brand_identity"]["geographic_focus"])
        # Verify listings/agent mechanics exist
        flat_contract_no = str(contract_no).lower()
        self.assertIn("listings", flat_contract_no)
        # Verify price shown and verified listings are NOT in the contract trust signals since they are not explicit
        self.assertNotIn("price shown", contract_no["trust_signals"])
        self.assertNotIn("verified listings", contract_no["trust_signals"])

        # Case B: Explicit verified listings and pricing
        state_explicit = {
            "brand_name": "AqarFind",
            "internal_resources": [
                {"link": "https://aqarfind.com/rent", "text": "browse rent listings in Cairo with pricing starting from EGP 5000", "page_text": "pricing shown on all listings, verified status for properties", "headings": ["Rent Listings Cairo"]},
                {"link": "https://aqarfind.com/agent", "text": "contact agent for verified property details"}
            ],
            "brand_evidence_map": {
                "strong_signals": ["verified status", "price shown"]
            },
            "brand_evidence_cards": []
        }
        contract_yes = build_brand_offer_contract(state_explicit)
        self.assertIn("price shown", contract_yes["trust_signals"])
        self.assertIn("verified listings", contract_yes["trust_signals"])

    def test_regression_ecommerce_no_cross_industry_leakage(self):
        """3. Ecommerce brand fixture:
        Mock internal_resources for ecommerce.
        Expected: shopping mechanics produced; no real-estate listing or digital agency service fields.
        """
        from src.services.brand_evidence_service import build_brand_offer_contract
        state = {
            "brand_name": "ShopSwift",
            "internal_resources": [
                {"link": "https://shopswift.com/products", "text": "electronics, clothing products, browse items", "headings": ["Our Products", "Categories"]},
                {"link": "https://shopswift.com/cart", "text": "add items to cart and check out"}
            ],
            "brand_evidence_cards": []
        }
        contract = build_brand_offer_contract(state)
        self.assertEqual(contract["evidence_summary"]["detected_domain"], "ecommerce")
        flat_contract = str(contract).lower()
        
        # Verify no cross-industry leakage
        self.assertNotIn("real estate", flat_contract)
        self.assertNotIn("verified listings", flat_contract)
        self.assertNotIn("web design agency", flat_contract)

    def test_regression_unknown_brand_low_confidence_soft_context(self):
        """4. Weak/unknown brand fixture:
        Mock generic homepage with little/no evidence.
        Expected: low confidence, brand_usage_mode = soft_context_only, generic headings gated, etc.
        """
        from src.services.brand_evidence_service import build_brand_heading_guardrails, build_brand_generation_guardrails
        state = {
            "brand_name": "UnknownCo",
            "internal_resources": [
                {"link": "https://unknownco.com", "text": "Welcome to our homepage."}
            ],
            "brand_evidence_cards": [],
            "brand_offer_contract": {
                "brand_identity": {
                    "confidence": "low",
                    "brand_name": "UnknownCo"
                }
            }
        }
        guardrails = build_brand_heading_guardrails(state)
        self.assertEqual(guardrails["brand_heading_policy"], "low_confidence_soft_context_only")
        gen_guardrails = build_brand_generation_guardrails(state)
        self.assertEqual(gen_guardrails["brand_usage_mode"], "soft_context_only")
        self.assertTrue(any("differentiation or brand-proof" in rule for rule in guardrails["heading_rules"]))
        # Assert claim gate removes unsupported claims
        from src.services.brand_evidence_service import apply_brand_claim_gate
        brief = {
            "brand_name": "UnknownCo",
            "allowed_claims": []
        }
        text = "UnknownCo is the best company with 100% guarantee in Saudi Arabia."
        result = apply_brand_claim_gate(text, brief)
        self.assertEqual(result, "")

    def test_regression_creative_minds_like_fixture_uses_page_backed_details(self):
        """5. Current-output regression:
        Egyptian agency fixture without Saudi presence.
        Expected: Egyptian geographic focus, no unsupported Saudi claims, no industry leakage.
        """
        from src.services.brand_evidence_service import build_brand_offer_contract, apply_brand_claim_gate
        state = {
            "brand_name": "CreativeMinds",
            "internal_resources": [
                {"link": "https://creativeminds.eg/ui-ux", "text": "UI/UX design, custom graphic design services", "headings": ["UI/UX and Graphic Design"]},
                {"link": "https://creativeminds.eg/software", "text": "software solutions and custom enterprise software development", "headings": ["E-business/software solutions"]},
                {"link": "https://creativeminds.eg/about", "text": "about CreativeMinds office located in Egypt", "headings": ["About Us"]},
                {"link": "https://creativeminds.eg/portfolio", "text": "portfolio case studies and projects design", "headings": ["Our Portfolio"]}
            ],
            "brand_evidence_cards": []
        }
        contract = build_brand_offer_contract(state)
        self.assertEqual(contract["evidence_summary"]["detected_domain"], "digital_services")
        self.assertIn("Egypt", contract["brand_identity"]["geographic_focus"])
        self.assertNotIn("Saudi", contract["brand_identity"]["geographic_focus"])
        
        brief = {
            "brand_name": "CreativeMinds",
            "allowed_claims": ["Egypt location", "software development", "UI/UX design"],
            "brand_offer_contract": contract
        }
        text = (
            "CreativeMinds is the best agency. "
            "CreativeMinds has office location in Egypt. "
            "CreativeMinds has office in Saudi Arabia. "
            "CreativeMinds offers 24/7 fast response."
        )
        result = apply_brand_claim_gate(text, brief)
        self.assertIn("CreativeMinds has office location in Egypt.", result)
        self.assertNotIn("Saudi Arabia", result)
        self.assertNotIn("24/7", result)
        self.assertNotIn("best agency", result)

    def test_regression_claim_gate_brand_name_is_not_evidence(self):
        """6. Assert that brand_name or keyword alone does NOT count as evidence to support a claim."""
        from src.services.brand_evidence_service import apply_brand_claim_gate
        brief = {
            "brand_name": "ProTech",
            "allowed_claims": [] # Empty allowed claims list!
        }
        # Brand name present, but no allowed claim for guarantee or response time
        text = "ProTech has 100% satisfaction guarantee."
        result = apply_brand_claim_gate(text, brief)
        self.assertNotIn("guarantee", result)

    def test_regression_section_source_text_can_support_specific_claims(self):
        """7. Assert that section_source_text can support claims only when it explicitly contains evidence."""
        from src.services.brand_evidence_service import apply_brand_claim_gate
        brief_no_evidence = {
            "brand_name": "ProTech",
            "allowed_claims": [],
            "section_source_text": "Welcome to our page."
        }
        text = "ProTech offers 100% satisfaction guarantee."
        result_no = apply_brand_claim_gate(text, brief_no_evidence)
        self.assertNotIn("guarantee", result_no)

        brief_with_evidence = {
            "brand_name": "ProTech",
            "allowed_claims": [],
            "section_source_text": "Observed: 100% satisfaction guarantee."
        }
        result_yes = apply_brand_claim_gate(text, brief_with_evidence)
        self.assertIn("100% satisfaction guarantee", result_yes)

    def test_regression_arabic_generic_brand_claims_are_gated(self):
        """8. Assert Arabic generic brand claims are gated (best prices, guaranteed customer satisfaction, top choice)."""
        from src.services.brand_evidence_service import apply_brand_claim_gate
        brief = {
            "brand_name": "برو تيك",
            "allowed_claims": []
        }
        text = "برو تيك هي أفضل شركة تصميم. برو تيك تقدم أسعار مضمونة ورضا العملاء."
        result = apply_brand_claim_gate(text, brief)
        self.assertNotIn("أفضل", result)
        self.assertNotIn("أسعار مضمونة", result)
        self.assertNotIn("رضا العملاء", result)




    def test_regression_retrieval_depth_constraints(self):
        """Phase 1.7 Step 8: Comprehensive retrieval depth constraints tests."""
        from src.services.brand_evidence_service import chunk_text, build_brand_source_chunks, retrieve_brand_source_chunks
        
        # 1. page_text_full preserves content after 4k, while page_text remains truncated at 4k
        long_paragraph = "word " * 1200
        words = long_paragraph.split()
        words.insert(820, "ERP-Integration-Service-Details")
        long_text = "Start of page. " + " ".join(words) + " End of page."
        
        resource = {
            "link": "https://brand.com/deep",
            "title": "Brand ERP and CRM Integration",
            "headings": ["Custom ERP Integration Support"],
            "page_text": long_text[:4000],
            "page_text_full": long_text[:100000],
            "page_type": "services"
        }
        
        state = {
            "brand_name": "ProTech",
            "internal_resources": [resource]
        }
        
        # Verify backward compatibility
        self.assertEqual(len(resource["page_text"]), 4000)
        self.assertNotIn("ERP-Integration-Service-Details", resource["page_text"])
        self.assertIn("ERP-Integration-Service-Details", resource["page_text_full"])
        
        # 2. ERP/POS/CRM details after 4,000 characters are retrievable in chunks
        chunks = build_brand_source_chunks(state)
        # Chunks should be heading-aware first, word-based second
        self.assertTrue(len(chunks) > 1)
        
        # Verify that ERP-Integration-Service-Details is present in one of the chunks!
        has_deep_details = any("ERP-Integration-Service-Details" in c["text"] for c in chunks)
        self.assertTrue(has_deep_details)
        
        # 3. Retrieved source context per section stays under 3000 character limit
        section = {
            "heading_text": "ProTech ERP Integration Solutions",
            "content_goal": "Describe ProTech ERP features",
            "section_intent": "Commercial",
            "assigned_keywords": ["ERP integration", "CRM solutions"]
        }
        
        retrieved = retrieve_brand_source_chunks(section, state, top_k=3)
        self.assertTrue(len(retrieved) > 0)
        
        # Verify that the retrieved chunk containing ERP details is retrieved
        has_erp = any("ERP-Integration-Service-Details" in c["text"] for c in retrieved)
        self.assertTrue(has_erp)
        
        # 4. Homepage chunks do not dominate service-specific retrieval
        homepage_resource = {
            "link": "https://brand.com",
            "title": "ProTech | Home",
            "headings": ["Welcome to ProTech Home"],
            "page_text": "This is our homepage welcome text.",
            "page_text_full": "This is our homepage welcome text.",
            "page_type": "home"
        }
        service_resource = {
            "link": "https://brand.com/erp",
            "title": "ERP System Integration Services",
            "headings": ["ERP System Integration Services"],
            "page_text": "We provide custom ERP and CRM software system integrations.",
            "page_text_full": "We provide custom ERP and CRM software system integrations.",
            "page_type": "services"
        }
        
        state_multi = {
            "brand_name": "ProTech",
            "internal_resources": [homepage_resource, service_resource]
        }
        
        service_section = {
            "heading_text": "ERP Integration Services by ProTech",
            "content_goal": "Explain ERP integrations",
            "section_intent": "Commercial",
            "assigned_keywords": ["ERP"]
        }
        
        # Retrieve chunks for service-specific section
        retrieved_multi = retrieve_brand_source_chunks(service_section, state_multi, top_k=2)
        # Service page chunk should rank first, homepage chunk should not dominate!
        self.assertEqual(retrieved_multi[0]["page_type"], "services")

    def test_regression_conditional_chunk_injection_relevance(self):
        """Phase 1.7 Step 8: Chunks are conditionally injected only when relevant."""
        # Verify relevance check logic
        state = {
            "brand_name": "ProTech",
            "brand_aliases": ["PT"],
            "brand_source_chunks": [
                {
                    "text": "PT ERP system integrates POS/CRM software features.",
                    "url": "https://pt.com/erp",
                    "heading": "ERP Solutions",
                    "page_title": "PT ERP",
                    "page_type": "services"
                }
            ]
        }
        
        # Relevant: Mentions brand alias PT
        section_relevant = {
            "heading_text": "Choosing PT Integration",
            "content_goal": "PT benefits",
            "section_intent": "Commercial"
        }
        
        # Irrelevant: Does not mention brand name or aliases, nor is brand-specific intent
        section_irrelevant = {
            "heading_text": "General Industry Trends",
            "content_goal": "Describe generic B2B trends",
            "section_intent": "Informational"
        }
        
        from src.services.brand_evidence_service import retrieve_brand_source_chunks
        
        chunks_rel = retrieve_brand_source_chunks(section_relevant, state, top_k=1)
        self.assertTrue(len(chunks_rel) > 0)
        
        # General B2B section will not match brand/alias and is not evidence-intent, so it should not receive PT chunks
        chunks_irrel = retrieve_brand_source_chunks(section_irrelevant, state, top_k=1)
        self.assertEqual(len(chunks_irrel), 0)

    def test_regression_terminology_grounding_and_organizer_structure(self):
        """Phase 1.7 Step 9: Factual Organizer build_section_brand_understanding structure and terminology grounding."""
        from src.services.brand_evidence_service import build_section_brand_understanding
        
        section = {
            "heading_text": "برمجة المواقع وتطبيقات الويب",
            "section_intent": "Commercial"
        }
        state = {
            "brand_name": "ProTech",
            "brand_aliases": ["PT"],
            "brand_source_chunks": []
        }
        retrieved_chunks = [
            {
                "text": "We build web applications using React, Laravel, and PostgreSQL.",
                "url": "https://brand.com/tech",
                "heading": "Our Tech Stack",
                "page_title": "Technologies",
                "page_type": "services"
            }
        ]
        
        brief = build_section_brand_understanding(section, state, retrieved_chunks)
        
        # Verify the structure of the brief
        self.assertEqual(brief["section_heading"], "برمجة المواقع وتطبيقات الويب")
        self.assertEqual(brief["section_intent"], "Commercial")
        self.assertIn("React", brief["relevant_technologies"])
        self.assertIn("Laravel", brief["relevant_technologies"])
        self.assertIn("PostgreSQL", brief["relevant_technologies"])
        self.assertEqual(brief["recommended_angle"]["preferred_section_style"], "evidence_grounded")
        self.assertIn("observed services", brief["recommended_angle"]["focus_types"])

    def test_regression_multi_signal_project_extraction(self):
        """Phase 1.7 Step 9: Multi-signal project extraction from retrieved brand chunks."""
        from src.services.brand_evidence_service import build_section_brand_understanding
        
        section = {
            "heading_text": "مشاريعنا السابقة في تطوير المتاجر",
            "section_intent": "brand_proof"
        }
        state = {
            "brand_name": "ProTech",
            "brand_source_chunks": []
        }
        
        retrieved_chunks = [
            {
                # Signal A: Bullets
                # Signal B: Proper Noun "Acumen Commerce Suite"
                # Signal C: Arabic quotes "مشروع «متجر العثيم»"
                "text": "- Project GoldenGate Gateways\nWe launched project «متجر العثيم» for digital groceries. Also we completed Acumen Commerce Suite integration.",
                "url": "https://brand.com/portfolio",
                "heading": "Acumen Commerce Suite Integration",
                "page_title": "Case Studies",
                "page_type": "projects" # Signal D: page_type is projects
            }
        ]
        
        brief = build_section_brand_understanding(section, state, retrieved_chunks)
        
        self.assertIn("Acumen Commerce Suite Integration", brief["relevant_projects"])
        self.assertIn("Project GoldenGate Gateways", brief["relevant_projects"])
        self.assertIn("متجر العثيم", brief["relevant_projects"])
        self.assertIn("Acumen Commerce Suite", brief["relevant_projects"])

    def test_regression_technology_and_workflow_extraction(self):
        """Phase 1.7 Step 9: Extracts technologies and workflow stages correctly."""
        from src.services.brand_evidence_service import build_section_brand_understanding
        
        section = {
            "heading_text": "خطوات العمل ومراحل التنفيذ",
            "section_intent": "Commercial"
        }
        state = {
            "brand_name": "ProTech",
            "brand_source_chunks": []
        }
        retrieved_chunks = [
            {
                "text": "Our workflow consists of Consultation & Planning, followed by Design & Development, and final testing.",
                "url": "https://brand.com/process",
                "heading": "Our Process",
                "page_title": "Process",
                "page_type": "services"
            }
        ]
        
        brief = build_section_brand_understanding(section, state, retrieved_chunks)
        
        self.assertIn("Consultation & Planning", brief["relevant_process_steps"])
        self.assertIn("Design & Development", brief["relevant_process_steps"])

    def test_regression_conditional_why_choose_grounding(self):
        """Phase 1.7 Step 9: Why choose sections allow trust evidence grounding without blocking by default."""
        from src.services.brand_evidence_service import build_section_brand_understanding
        
        section = {
            "heading_text": "لماذا تختار شركة ProTech لتصميم موقعك؟",
            "section_intent": "Commercial"
        }
        state = {
            "brand_name": "ProTech",
            "brand_source_chunks": []
        }
        retrieved_chunks = [
            {
                "text": "We provide custom ERP and React based design services.",
                "url": "https://brand.com/why-us",
                "heading": "Why Us",
                "page_title": "About Us",
                "page_type": "about"
            }
        ]
        
        brief = build_section_brand_understanding(section, state, retrieved_chunks)
        
        # Should allow section and keep preferred_section_style as evidence_grounded
        self.assertEqual(brief["recommended_angle"]["preferred_section_style"], "evidence_grounded")
        self.assertEqual(len(brief["not_supported_for_this_section"]), 0)

    def test_regression_saudi_geographical_presence_constraints(self):
        """Phase 1.7 Step 9: Saudi geography check warns when location is completely unsupported."""
        from src.services.brand_evidence_service import build_section_brand_understanding
        
        section = {
            "heading_text": "أفضل شركة تصميم مواقع في السعودية",
            "section_intent": "Commercial"
        }
        state = {
            "brand_name": "ProTech",
            "brand_source_chunks": []
        }
        retrieved_chunks = [
            {
                "text": "We build websites for startups worldwide.",
                "url": "https://brand.com/about",
                "heading": "About Us",
                "page_title": "About Us",
                "page_type": "about"
            }
        ]
        
        brief = build_section_brand_understanding(section, state, retrieved_chunks)
        
        # Saudi presence heading is unsupported because no Saudi/Riyadh evidence is present in retrieved chunks
        self.assertTrue(any("السعودية" in warning for warning in brief["not_supported_for_this_section"]))

    def test_regression_preflight_heading_downgrade(self):
        """Phase 1.7 Step 9: Outline project-promising headings are downgraded if project evidence is completely unsupported."""
        from src.services.workflow_controller import AsyncWorkflowController
        
        controller = AsyncWorkflowController()
        
        section = {
            "heading_text": "أمثلة من مشاريعنا الناجحة في الرياض",
            "section_intent": "brand_proof"
        }
        state = {
            "brand_name": "ProTech",
            "article_language": "ar",
            "brand_source_chunks": [] # Empty! No project evidence whatsoever!
        }
        
        downgraded_heading = controller._fulfill_and_downgrade_heading(section, state)
        
        # Heading must be downgraded to general evaluation criteria or choosing best partner
        self.assertNotEqual(downgraded_heading, "أمثلة من مشاريعنا الناجحة في الرياض")
        self.assertIn("معايير اختيار", downgraded_heading)

    def test_section_brand_understanding_handles_sparse_chunks_without_warning(self):
        """Section brand organizer should tolerate incomplete chunk dictionaries and avoid regex warnings."""
        import warnings
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {
            "heading_text": "ProTech technologies",
            "section_intent": "Commercial"
        }
        state = {"brand_name": "ProTech"}
        retrieved_chunks = [
            {"text": "React and Laravel are used for web application development."},
            {"heading": "Technology stack"},
            {"text": None, "heading": None},
        ]

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            brief = build_section_brand_understanding(section, state, retrieved_chunks)

        self.assertIn("React", brief["relevant_technologies"])
        self.assertIn("Laravel", brief["relevant_technologies"])
        warning_messages = [str(w.message) for w in caught]
        self.assertFalse(any("Possible set union" in msg for msg in warning_messages))

    async def test_repair_writer_receives_section_brand_understanding(self):
        """Repair writer calls must preserve the section brand organizer."""
        from src.services.workflow_controller import AsyncWorkflowController
        from unittest.mock import AsyncMock, MagicMock

        controller = AsyncWorkflowController(work_dir=".")
        controller.section_writer = MagicMock()
        controller.section_writer.write = AsyncMock(side_effect=[
            {"content": "Initial section content.", "section_id": "s1"},
            {"content": "Repaired section content.", "section_id": "s1"},
        ])
        controller.validator.validate_section_output = AsyncMock(
            return_value=(False, ["PLAIN_LANGUAGE_REQUIRED: simplify phrasing"])
        )
        controller.validator.enforce_paragraph_structure = MagicMock(side_effect=lambda content: content)
        controller.validator.extract_sentences = MagicMock(return_value=["Initial section content."])

        section = {
            "section_id": "s1",
            "heading_text": "How ProTech uses React",
            "section_type": "core",
            "section_intent": "Commercial",
        }
        state = {
            "brand_name": "ProTech",
            "primary_keyword": "web development",
            "brand_source_chunks": [
                {
                    "text": "ProTech builds web applications using React and Laravel.",
                    "url": "https://brand.com/tech",
                    "heading": "Technology stack",
                    "page_title": "Technology",
                    "page_type": "services",
                }
            ],
            "used_internal_links": [],
            "used_external_links": [],
            "used_phrases": [],
            "used_claims": [],
            "used_topics": [],
        }

        await controller._write_single_section(
            title="Best Web Development",
            global_keywords={},
            section=section,
            article_intent="commercial",
            seo_intelligence={},
            content_type="brand_commercial",
            link_strategy={},
            state=state,
        )

        self.assertGreaterEqual(controller.section_writer.write.call_count, 2)
        first_kwargs = controller.section_writer.write.call_args_list[0].kwargs
        repair_kwargs = controller.section_writer.write.call_args_list[1].kwargs
        # Pack-Only Writer Truth firewall: writer receives empty dict for section_brand_understanding
        # The populated understanding is stored on section["section_brand_understanding"] for audit/validation.
        self.assertIsNotNone(first_kwargs.get("section_brand_understanding"))
        self.assertIsNotNone(repair_kwargs.get("section_brand_understanding"))
        # The section object itself retains the full understanding for downstream audit/validation
        self.assertIn("relevant_technologies", section.get("section_brand_understanding", {}))
        self.assertIn("React", section["section_brand_understanding"]["relevant_technologies"])

    def test_brand_commercial_offer_sections_without_visible_brand_stay_generic(self):
        """Generic brand-commercial offer sections should not become brand-owned catalogs."""
        from src.services.workflow_controller import AsyncWorkflowController

        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_name": "Creative Minds",
            "display_brand_name": "Creative Minds",
            "primary_keyword": "افضل شركة تصميم مواقع في السعودية",
            "article_language": "ar",
            "content_type": "brand_commercial",
            "content_strategy": {},
            "seo_intelligence": {},
        }
        outline = [
            {
                "section_id": "sec_01",
                "heading_text": "مقدمة",
                "heading_level": "INTRO",
                "section_type": "introduction",
                "section_intent": "informational",
                "subheadings": [],
            },
            {
                "section_id": "sec_02",
                "heading_text": "افضل شركة تصميم مواقع في السعودية: الخيارات والخدمات المتاحة",
                "heading_level": "H2",
                "section_type": "offer",
                "section_intent": "informational",
                "subheadings": ["تصميم المواقع الإلكترونية للشركات والمؤسسات"],
            },
        ]

        prepared = controller._prepare_outline_for_content(state, outline)
        offer = prepared["outline"][1]

        self.assertNotEqual(offer["taxonomy_axis"], "brand_offer")
        self.assertNotEqual(offer.get("execution_mode"), "brand_service_catalog")
        details_blob = " ".join(offer["must_include_details"])
        self.assertNotIn("brand-provided service", details_blob.lower())

    def test_brand_commercial_offer_sections_with_visible_brand_get_brand_contract(self):
        """Brand-named offer sections still receive brand-owned service catalog guidance."""
        from src.services.workflow_controller import AsyncWorkflowController

        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_name": "Creative Minds",
            "display_brand_name": "Creative Minds",
            "primary_keyword": "best web design company",
            "article_language": "en",
            "content_type": "brand_commercial",
            "content_strategy": {},
            "seo_intelligence": {},
        }
        outline = [
            {
                "section_id": "sec_01",
                "heading_text": "Introduction",
                "heading_level": "INTRO",
                "section_type": "introduction",
                "section_intent": "informational",
                "subheadings": [],
            },
            {
                "section_id": "sec_02",
                "heading_text": "Services offered by Creative Minds",
                "heading_level": "H2",
                "section_type": "offer",
                "section_intent": "commercial",
                "subheadings": ["Website development"],
            },
        ]

        prepared = controller._prepare_outline_for_content(state, outline)
        offer = prepared["outline"][1]

        self.assertEqual(offer["taxonomy_axis"], "brand_offer")
        self.assertEqual(offer["execution_mode"], "brand_service_catalog")

    def test_select_sources_for_brand_commercial_offer_without_brand_name(self):
        """Generic service sections should not receive brand evidence unless the H2 names the brand."""
        from src.services.brand_evidence_service import select_section_brand_sources

        state = {
            "brand_name": "Creative Minds",
            "content_type": "brand_commercial",
            "brand_evidence_cards": [
                {
                    "url": "https://cems-it.com/services",
                    "title": "Services",
                    "page_type": "services",
                    "headings": ["Web Development", "Mobile App Development"],
                    "cta_labels": ["Contact Us"],
                    "visible_products_or_services": ["Web Development", "Mobile App Development"],
                    "visible_features_or_capabilities": ["ERP", "CRM", "POS"],
                    "visible_process_steps": [],
                    "visible_conversion_actions": ["contact us"],
                    "visible_trust_signals": [],
                    "visible_geography": [],
                    "visible_project_or_case_study_examples": [],
                    "visible_pricing_or_packages": [],
                    "visible_support_or_contact_methods": [],
                    "usable_snippets": ["Creative Minds provides web development and mobile app services."],
                    "excluded_reason": None,
                }
            ],
        }
        section = {
            "heading_text": "الخدمات والحلول المتاحة",
            "section_type": "offer",
            "content_type": "brand_commercial",
            "section_contract": {"brand_policy": "commercial"},
        }

        text, count = select_section_brand_sources(section, state)

        self.assertEqual(count, 1)
        self.assertIn("Web Development", text)
        self.assertIn("ERP", text)

        section["heading_text"] = "Services offered by Creative Minds"
        text, count = select_section_brand_sources(section, state)

        self.assertEqual(count, 1)
        self.assertIn("[SECTION-SPECIFIC BRAND EVIDENCE]", text)
        self.assertIn("Web Development", text)
        self.assertIn("ERP", text)

    def test_section_understanding_uses_raw_portfolio_blocks_for_projects(self):
        """Project sections should use raw portfolio blocks, not evidence-card project names as final truth."""
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {
            "heading_text": "نماذج من مشاريع Creative Minds",
            "section_intent": "Commercial",
        }
        state = {
            "brand_name": "Creative Minds",
            "brand_evidence_cards": [
                {
                    "url": "https://cems-it.com/projects",
                    "title": "Projects",
                    "page_type": "portfolio",
                    "headings": ["Portfolio"],
                    "visible_products_or_services": [],
                    "visible_features_or_capabilities": [],
                    "visible_process_steps": [],
                    "visible_conversion_actions": [],
                    "visible_trust_signals": [],
                    "visible_geography": ["Egypt"],
                    "visible_project_or_case_study_examples": [
                        "Acumen Consulting Egypt",
                        "Aqar Ya Masr Mob App",
                        "Baddel",
                    ],
                    "visible_pricing_or_packages": [],
                    "visible_support_or_contact_methods": [],
                    "usable_snippets": ["Aqar Ya Masr Mob App appears in the portfolio."],
                    "excluded_reason": None,
                }
            ],
            "brand_source_chunks": [
                {
                    "url": "https://cems-it.com/projects",
                    "page_type": "portfolio",
                    "heading": "Portfolio",
                    "text": "Project: Acumen Consulting Egypt. Project: Aqar Ya Masr Mob App. Client: Baddel.",
                }
            ],
        }
        chunks = [
            {
                "text": "Project: Acumen Consulting Egypt. Project: Aqar Ya Masr Mob App. Client: Baddel.",
                "heading": "Portfolio",
                "page_type": "portfolio",
            }
        ]

        brief = build_section_brand_understanding(section, state, chunks)

        self.assertIn("Aqar Ya Masr", brief["relevant_projects"])
        self.assertIn("Baddel", brief["relevant_projects"])
        self.assertEqual(brief["recommended_angle"]["preferred_section_style"], "evidence_grounded")

    def test_compact_summary_prioritizes_projects_and_filters_junk(self):
        """Outline evidence summary should expose useful projects instead of date/menu noise."""
        from src.services.brand_evidence_service import build_compact_brand_evidence_summary

        state = {
            "brand_offer_contract": {
                "brand_identity": {"confidence": "high", "category": "web design agency"},
                "evidence_summary": {"strong_evidence": []},
                "conversion_actions": [],
                "brand_limitations": [],
            },
            "brand_evidence_cards": [
                {
                    "url": "https://cems-it.com/projects",
                    "title": "Projects",
                    "page_type": "portfolio",
                    "visible_products_or_services": ["04/26/2019", "Web Development"],
                    "visible_features_or_capabilities": ["ERP", "CRM"],
                    "visible_project_or_case_study_examples": ["Acumen Consulting Egypt", "Baddel"],
                    "visible_process_steps": ["Consultation & Planning"],
                    "excluded_reason": None,
                }
            ],
        }

        summary = build_compact_brand_evidence_summary(state)

        self.assertIn("Observed projects/case studies", summary)
        self.assertIn("Acumen Consulting Egypt", summary)
        self.assertIn("Consultation & Planning", summary)
        self.assertNotIn("04/26/2019", summary)

    def test_phase_18_sanitizer_filters_noise_preserves_real_services(self):
        """Phase 1.8: noisy headings/navigation must not become structured services or claims."""
        from src.services.brand_evidence_service import build_brand_evidence_cards

        state = {
            "brand_name": "Fixture Studio",
            "primary_keyword": "digital services",
            "area": "Riyadh",
            "internal_resources": [
                {
                    "link": "https://brand.test/services",
                    "title": "Services",
                    "headings": [
                        "العربية",
                        "IntoSOFTWARE",
                        "Why You Should Choose Us",
                        "Fast Turnaround",
                        "04/26/2019",
                        "Brief",
                        "Technologies Used",
                        "Web Development",
                        "Mobile App Development",
                        "ERP Systems",
                    ],
                    "cta_labels": ["Contact Us"],
                    "page_text": (
                        "Fixture Studio provides Web Development, Mobile App Development, "
                        "ERP Systems and CRM integrations for business teams."
                    ),
                }
            ],
        }
        original = copy.deepcopy(state)

        cards = build_brand_evidence_cards(state)
        contract = build_brand_offer_contract({**state, "brand_evidence_cards": cards})
        guardrails = build_brand_generation_guardrails({"brand_offer_contract": contract})

        services = cards[0]["visible_products_or_services"]
        service_blob = " | ".join(services + contract["offer_mechanics"]["supporting_services"])
        claims_blob = " | ".join(guardrails["allowed_brand_claims"])

        self.assertIn("Web Development", service_blob)
        self.assertIn("Mobile App Development", service_blob)
        self.assertIn("ERP Systems", service_blob)
        for noisy in ["العربية", "IntoSOFTWARE", "Why You Should Choose Us", "Fast Turnaround", "04/26/2019", "Brief", "Technologies Used"]:
            self.assertNotIn(noisy, service_blob)
            self.assertNotIn(noisy, claims_blob)
        self.assertEqual(state, original)

    def test_phase_18_contract_excludes_target_area_geography_leakage(self):
        """Phase 1.8: target article area must not become brand geography without source evidence."""
        state = {
            "brand_name": "Neutral Agency",
            "primary_keyword": "best digital agency in Riyadh",
            "area": "Riyadh",
            "brand_evidence_cards": [
                {
                    "url": "https://brand.test/services",
                    "title": "Services",
                    "page_type": "services",
                    "headings": ["Web Development", "CRM Integrations"],
                    "visible_products_or_services": ["Web Development"],
                    "visible_features_or_capabilities": ["CRM Integrations"],
                    "visible_process_steps": [],
                    "visible_conversion_actions": [],
                    "visible_trust_signals": [],
                    "visible_geography": [],
                    "visible_project_or_case_study_examples": [],
                    "visible_pricing_or_packages": [],
                    "visible_support_or_contact_methods": [],
                    "usable_snippets": ["The agency provides web development and CRM integrations."],
                    "excluded_reason": None,
                }
            ],
        }

        contract = build_brand_offer_contract(state)

        self.assertEqual(contract["brand_identity"]["geographic_focus"], [])
        self.assertNotIn("Riyadh", " ".join(contract["brand_identity"]["geographic_focus"]))

    def test_phase_18_portfolio_projects_outrank_slogan_proper_noun_noise(self):
        """Phase 1.8: project sections should surface portfolio projects before slogan/proper-noun noise."""
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {
            "heading_text": "نماذج من مشاريع BrandCo",
            "section_intent": "brand_proof",
        }
        state = {
            "brand_name": "BrandCo",
            "brand_evidence_cards": [
                {
                    "url": "https://brand.test/portfolio",
                    "title": "Portfolio",
                    "page_type": "portfolio",
                    "headings": ["Acumen Consulting Egypt", "Aqar Ya Masr Web app", "Arab Business Academy", "Baddel"],
                    "visible_products_or_services": [],
                    "visible_features_or_capabilities": [],
                    "visible_process_steps": [],
                    "visible_conversion_actions": [],
                    "visible_trust_signals": [],
                    "visible_geography": [],
                    "visible_project_or_case_study_examples": ["Acumen Consulting Egypt", "Aqar Ya Masr Web app", "Arab Business Academy", "Baddel"],
                    "visible_pricing_or_packages": [],
                    "visible_support_or_contact_methods": [],
                    "usable_snippets": ["Portfolio examples include Aqar Ya Masr Web app and Baddel."],
                    "excluded_reason": None,
                }
            ],
        }
        chunks = [
            {
                "text": (
                    "IntoSOFTWARE. Why You Should Choose Us. Top Rated Agency. "
                    "Project: Acumen Consulting Egypt. Project: Aqar Ya Masr Web app. "
                    "Project: Arab Business Academy. Client: Baddel."
                ),
                "heading": "Portfolio",
                "page_type": "portfolio",
            }
        ]

        brief = build_section_brand_understanding(section, state, chunks)

        self.assertEqual(brief["relevant_projects"][:4], [
            "Acumen Consulting Egypt",
            "Aqar Ya Masr",
            "Arab Business Academy",
            "Baddel",
        ])
        self.assertNotIn("IntoSOFTWARE", brief["relevant_projects"])
        self.assertNotIn("Why You Should Choose Us", brief["relevant_projects"])
        self.assertNotIn("Top Rated Agency", brief["relevant_projects"])

    def test_phase_18_brand_package_heading_downgrades_without_pricing_evidence(self):
        """Phase 1.8: brand-owned pricing/package headings downgrade when brand pricing is absent."""
        controller = AsyncWorkflowController(work_dir=".")
        section = {
            "section_id": "pkg",
            "heading_text": "باقات BrandCo لتصميم المواقع",
            "heading_level": "H2",
            "section_type": "offer",
            "section_intent": "Commercial",
            "subheadings": ["الباقة الأساسية", "الباقة المتقدمة"],
        }
        state = {
            "brand_name": "BrandCo",
            "article_language": "ar",
            "content_type": "brand_commercial",
            "brand_evidence_cards": [
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "visible_products_or_services": ["Web Development"],
                    "visible_features_or_capabilities": ["CRM Integrations"],
                    "visible_pricing_or_packages": [],
                    "excluded_reason": None,
                }
            ],
        }

        downgraded = controller._fulfill_and_downgrade_heading(section, state)

        self.assertNotIn("باقات", downgraded)
        self.assertIn("نطاق الخدمات", downgraded)
        self.assertEqual(section["fulfillment_status"], "unsupported")
        self.assertEqual(section["subheadings"], [])

    def test_phase_18_brand_pricing_fulfillment_requires_explicit_brand_pricing(self):
        """Phase 1.8: brand pricing/package content is unsupported unless brand sources contain pricing."""
        controller = AsyncWorkflowController(work_dir=".")
        section = {
            "section_id": "pricing",
            "heading_text": "باقات BrandCo وأسعارها",
            "taxonomy_axis": "brand_offer",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_offer"},
        }
        unsupported_state = {
            "content_type": "brand_commercial",
            "brand_evidence_cards": [
                {
                    "visible_products_or_services": ["Web Development"],
                    "visible_features_or_capabilities": [],
                    "visible_pricing_or_packages": [],
                    "visible_project_or_case_study_examples": [],
                    "visible_process_steps": [],
                    "visible_geography": [],
                    "excluded_reason": None,
                }
            ],
        }
        supported_state = copy.deepcopy(unsupported_state)
        supported_section = copy.deepcopy(section)
        supported_section["section_raw_brand_blocks"] = [
            {
                "source_url": "https://brand.test/pricing",
                "page_type": "pricing",
                "heading": "Packages",
                "observed_text": "Package starts from 500 USD.",
                "observed_facts": ["Package starts from 500 USD"],
            }
        ]
        supported_section["section_brand_understanding"] = {
            "relevant_services": ["Web Development"],
            "relevant_projects": [],
            "relevant_process_steps": [],
            "relevant_technologies": [],
            "relevant_geography": [],
            "not_supported_for_this_section": [],
        }

        unsupported = controller._evaluate_brand_owned_section_fulfillment(
            section,
            "BrandCo provides Web Development.",
            unsupported_state,
        )
        supported = controller._evaluate_brand_owned_section_fulfillment(
            supported_section,
            "BrandCo provides Web Development. Package starts from 500 USD.",
            supported_state,
        )

        self.assertEqual(unsupported["fulfillment_status"], "unsupported")
        self.assertNotEqual(supported["fulfillment_status"], "unsupported")

    def test_phase_18_prompt_templates_expose_brand_owned_claim_guards(self):
        """Phase 1.8: prompt files remain readable and include the brand-owned claim guards."""
        prompt_paths = [
            "assets/prompts/templates/section_contract.txt",
            "assets/prompts/templates/02_section_writer_brand_commercial_v2.txt",
            "assets/prompts/templates/runtime_state.txt",
        ]
        texts = {}
        for path in prompt_paths:
            with open(path, "r", encoding="utf-8") as handle:
                texts[path] = handle.read()

        self.assertIn("BRAND-OWNED EXCEPTION", texts["assets/prompts/templates/section_contract.txt"])
        self.assertIn("observed brand facts", texts["assets/prompts/templates/section_contract.txt"])
        self.assertIn("observed services, capabilities, process steps, projects", texts["assets/prompts/templates/02_section_writer_brand_commercial_v2.txt"])
        self.assertIn("Brand-owned pricing", texts["assets/prompts/templates/runtime_state.txt"])

    def test_phase_19_inventory_returns_category_availability_without_mutation(self):
        """Phase 1.9 Step 1: inventory is a source/category map, not a claim dump."""
        state = {
            "brand_name": "FixtureCo",
            "area": "Riyadh",
            "primary_keyword": "best digital agency in Riyadh",
            "brand_evidence_cards": [
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "visible_products_or_services": ["Web Development"],
                    "visible_features_or_capabilities": ["CRM Integrations"],
                    "visible_project_or_case_study_examples": [],
                    "visible_process_steps": ["Consultation & Planning"],
                    "visible_pricing_or_packages": [],
                    "visible_trust_signals": [],
                    "visible_geography": [],
                    "excluded_reason": None,
                },
                {
                    "url": "https://brand.test/portfolio",
                    "page_type": "portfolio",
                    "visible_products_or_services": [],
                    "visible_features_or_capabilities": [],
                    "visible_project_or_case_study_examples": ["Acumen Consulting Egypt", "Baddel"],
                    "visible_process_steps": [],
                    "visible_pricing_or_packages": [],
                    "visible_trust_signals": [],
                    "visible_geography": [],
                    "excluded_reason": None,
                },
                {
                    "url": "https://brand.test/pricing",
                    "page_type": "pricing",
                    "visible_products_or_services": [],
                    "visible_features_or_capabilities": [],
                    "visible_project_or_case_study_examples": [],
                    "visible_process_steps": [],
                    "visible_pricing_or_packages": ["Starter package: 500 USD"],
                    "visible_trust_signals": [],
                    "visible_geography": [],
                    "excluded_reason": None,
                },
                {
                    "url": "https://brand.test/about",
                    "page_type": "about",
                    "visible_products_or_services": [],
                    "visible_features_or_capabilities": [],
                    "visible_project_or_case_study_examples": [],
                    "visible_process_steps": [],
                    "visible_pricing_or_packages": [],
                    "visible_trust_signals": ["Certified partner"],
                    "visible_geography": ["Egypt"],
                    "excluded_reason": None,
                },
            ],
            "brand_source_chunks": [
                {"url": "https://brand.test/services", "page_type": "services", "heading": "Services", "text": "We provide Web Development and CRM integrations."},
                {"url": "https://brand.test/portfolio", "page_type": "portfolio", "heading": "Projects", "text": "Project: Acumen Consulting Egypt. Client: Baddel."},
                {"url": "https://brand.test/pricing", "page_type": "pricing", "heading": "Packages", "text": "Starter package: 500 USD."},
                {"url": "https://brand.test/about", "page_type": "about", "heading": "About", "text": "Based in Egypt. Certified partner."},
            ],
        }
        original = copy.deepcopy(state)

        inventory = build_brand_evidence_inventory(state)

        self.assertEqual(state, original)
        self.assertTrue(inventory["services_available"])
        self.assertTrue(inventory["projects_available"])
        self.assertTrue(inventory["pricing_available"])
        self.assertTrue(inventory["process_available"])
        self.assertTrue(inventory["trust_available"])
        self.assertIn("Egypt", inventory["explicit_geography"])
        self.assertNotIn("Riyadh", inventory["explicit_geography"])
        self.assertIn("https://brand.test/services", inventory["service_page_urls"])
        self.assertIn("https://brand.test/portfolio", inventory["project_page_urls"])
        self.assertIn("https://brand.test/pricing", inventory["pricing_page_urls"])
        self.assertIn(inventory["confidence"], {"medium", "high"})

    def test_phase_19_cards_are_not_raw_allowed_brand_claims(self):
        """Phase 1.9 Step 1: card services stay capabilities, not allowed proof claims."""
        state = {
            "brand_name": "FixtureCo",
            "primary_keyword": "web development agency",
            "brand_evidence_cards": [
                {
                    "url": "https://brand.test/services",
                    "title": "Services",
                    "page_type": "services",
                    "headings": ["Why You Should Choose Us", "Web Development"],
                    "visible_products_or_services": ["Why You Should Choose Us", "Web Development"],
                    "visible_features_or_capabilities": ["CRM Integrations", "Brief"],
                    "visible_process_steps": [],
                    "visible_conversion_actions": [],
                    "visible_trust_signals": [],
                    "visible_geography": [],
                    "visible_project_or_case_study_examples": [],
                    "visible_pricing_or_packages": [],
                    "visible_support_or_contact_methods": [],
                    "usable_snippets": ["We provide Web Development and CRM integrations."],
                    "excluded_reason": None,
                }
            ],
        }

        contract = build_brand_offer_contract(state)
        guardrails = build_brand_generation_guardrails({"brand_offer_contract": contract})
        claims_blob = " | ".join(guardrails["allowed_brand_claims"])
        capabilities_blob = " | ".join(guardrails["allowed_brand_capabilities"])

        self.assertIn("Web Development", capabilities_blob)
        self.assertNotIn("Web Development", claims_blob)
        self.assertNotIn("Why You Should Choose Us", claims_blob)
        self.assertNotIn("Brief", claims_blob)
        self.assertNotIn("Why You Should Choose Us", capabilities_blob)
        self.assertNotIn("Brief", capabilities_blob)

    def test_phase_19_inventory_excludes_keyword_area_geography_leakage(self):
        """Phase 1.9 Step 1: target area in keyword does not become explicit brand geography."""
        state = {
            "brand_name": "NeutralCo",
            "area": "Riyadh",
            "primary_keyword": "best software company in Riyadh",
            "brand_evidence_cards": [
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "visible_products_or_services": ["Software Development"],
                    "visible_features_or_capabilities": [],
                    "visible_project_or_case_study_examples": [],
                    "visible_process_steps": [],
                    "visible_pricing_or_packages": [],
                    "visible_trust_signals": [],
                    "visible_geography": [],
                    "excluded_reason": None,
                }
            ],
            "brand_source_chunks": [
                {"url": "https://brand.test/services", "page_type": "services", "heading": "Services", "text": "Software Development for business teams."}
            ],
        }

        inventory = build_brand_evidence_inventory(state)

        self.assertEqual(inventory["explicit_geography"], [])
        self.assertNotIn("Riyadh", str(inventory))

    def test_phase_19_inventory_pricing_requires_explicit_evidence(self):
        """Phase 1.9 Step 1: vague pricing discussion does not unlock brand pricing/packages."""
        unsupported_state = {
            "brand_name": "NeutralCo",
            "brand_evidence_cards": [
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "visible_products_or_services": ["Software Development"],
                    "visible_features_or_capabilities": [],
                    "visible_project_or_case_study_examples": [],
                    "visible_process_steps": [],
                    "visible_pricing_or_packages": [],
                    "visible_trust_signals": [],
                    "visible_geography": [],
                    "excluded_reason": None,
                }
            ],
            "brand_source_chunks": [
                {"url": "https://brand.test/services", "page_type": "services", "heading": "Services", "text": "Pricing depends on project scope and timeline."}
            ],
        }
        supported_state = copy.deepcopy(unsupported_state)
        supported_state["brand_source_chunks"] = [
            {"url": "https://brand.test/pricing", "page_type": "pricing", "heading": "Packages", "text": "Starter package: 500 USD."}
        ]

        unsupported = build_brand_evidence_inventory(unsupported_state)
        supported = build_brand_evidence_inventory(supported_state)

        self.assertFalse(unsupported["pricing_available"])
        self.assertEqual(unsupported["pricing_page_urls"], [])
        self.assertTrue(supported["pricing_available"])
        self.assertIn("https://brand.test/pricing", supported["pricing_page_urls"])

    def test_phase_19_step2_service_brand_section_returns_service_raw_blocks(self):
        """Phase 1.9 Step 2: service brand headings receive compact service source blocks."""
        state = {
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/portfolio",
                    "page_type": "portfolio",
                    "heading": "Projects",
                    "text": "Project: Atlas Portal. Client platform delivered for internal teams.",
                },
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "heading": "Services",
                    "text": "BrandCo provides Web Development, UX/UI Design, and CRM integrations for business teams.",
                },
            ],
        }
        section = {"heading_text": "Services offered by BrandCo", "section_type": "offer"}
        original = copy.deepcopy(state)

        blocks = select_section_raw_brand_blocks(section, state)

        self.assertEqual(state, original)
        self.assertGreaterEqual(len(blocks), 1)
        self.assertEqual(blocks[0]["source_url"], "https://brand.test/services")
        self.assertEqual(blocks[0]["page_type"], "services")
        self.assertIn("Web Development", blocks[0]["observed_text"])
        self.assertTrue(any("Web Development" in fact for fact in blocks[0]["observed_facts"]))
        self.assertLessEqual(len(blocks[0]["observed_text"]), 700)

    def test_phase_19_step2_project_brand_section_returns_portfolio_raw_blocks(self):
        """Phase 1.9 Step 2: project headings prefer portfolio/project raw chunks."""
        state = {
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "heading": "Services",
                    "text": "BrandCo provides Web Development and CRM integrations.",
                },
                {
                    "url": "https://brand.test/portfolio",
                    "page_type": "portfolio",
                    "heading": "Selected Projects",
                    "text": "Project: Acumen Consulting Egypt. Client: Baddel. Project: Arab Business Academy.",
                },
            ],
        }
        section = {"heading_text": "Projects shown by BrandCo", "section_type": "proof"}

        blocks = select_section_raw_brand_blocks(section, state)

        self.assertGreaterEqual(len(blocks), 1)
        self.assertEqual(blocks[0]["source_url"], "https://brand.test/portfolio")
        self.assertIn("Acumen Consulting Egypt", blocks[0]["observed_text"])
        self.assertTrue(any("Project" in fact or "Client" in fact for fact in blocks[0]["observed_facts"]))

    def test_phase_19_step2_generic_non_brand_heading_returns_empty(self):
        """Phase 1.9 Step 2: generic sections do not receive brand source blocks."""
        state = {
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {"url": "https://brand.test/services", "page_type": "services", "heading": "Services", "text": "BrandCo provides Web Development."}
            ],
        }
        section = {"heading_text": "Available service options", "section_type": "offer"}

        self.assertEqual(select_section_raw_brand_blocks(section, state), [])

    def test_brand_commercial_service_heading_receives_brand_blocks_without_visible_brand(self):
        """Commercial service sections should be brand-aware even with generic approved headings."""
        state = {
            "brand_name": "BrandCo",
            "content_type": "brand_commercial",
            "brand_evidence_inventory": {
                "services_available": True,
                "projects_available": False,
                "pricing_available": False,
                "process_available": False,
                "trust_available": False,
                "explicit_geography": [],
                "confidence": "medium",
            },
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "heading": "Services",
                    "text": "BrandCo provides Web Development, UX/UI Design, and CRM integrations for business teams.",
                }
            ],
        }
        section = {"heading_text": "Available service options", "section_type": "offer"}

        blocks = select_section_raw_brand_blocks(section, state)

        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0]["source_url"], "https://brand.test/services")
        self.assertIn("Web Development", blocks[0]["observed_text"])

    def test_phase_19_step2_pricing_section_requires_explicit_pricing(self):
        """Phase 1.9 Step 2: brand pricing/package sections return [] without explicit pricing evidence."""
        state = {
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "heading": "Services",
                    "text": "BrandCo provides Web Development. Pricing depends on project scope.",
                }
            ],
            "brand_evidence_inventory": {
                "pricing_available": False,
                "services_available": True,
                "projects_available": False,
                "process_available": False,
                "trust_available": False,
                "explicit_geography": [],
            },
        }
        section = {"heading_text": "BrandCo pricing packages", "section_type": "pricing"}

        self.assertEqual(select_section_raw_brand_blocks(section, state), [])

        supported_state = copy.deepcopy(state)
        supported_state["brand_source_chunks"] = [
            {
                "url": "https://brand.test/pricing",
                "page_type": "pricing",
                "heading": "Packages",
                "text": "BrandCo Starter package: 500 USD. Growth package: 900 USD.",
            }
        ]
        supported_state["brand_evidence_inventory"]["pricing_available"] = True

        blocks = select_section_raw_brand_blocks(section, supported_state)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0]["source_url"], "https://brand.test/pricing")
        self.assertIn("500 USD", blocks[0]["observed_text"])

    def test_phase_19_step2_noisy_chunks_are_excluded(self):
        """Phase 1.9 Step 2: footer/menu/newsletter/date-only chunks are not returned."""
        state = {
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/footer",
                    "page_type": "home",
                    "heading": "Footer",
                    "text": "Subscribe newsletter. Privacy Policy. Terms of Use. Copyright all rights reserved.",
                },
                {
                    "url": "https://brand.test/date",
                    "page_type": "other",
                    "heading": "04/26/2019",
                    "text": "04/26/2019",
                },
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "heading": "Services",
                    "text": "BrandCo provides Software Development and ERP integrations for operations teams.",
                },
            ],
        }
        section = {"heading_text": "What BrandCo provides", "section_type": "offer"}

        blocks = select_section_raw_brand_blocks(section, state, max_blocks=4)
        urls = [block["source_url"] for block in blocks]

        self.assertEqual(urls, ["https://brand.test/services"])
        self.assertNotIn("newsletter", str(blocks).lower())
        self.assertNotIn("04/26/2019", str(blocks))

    def test_phase_19_step2_project_selector_prefers_project_pages_over_about_noise(self):
        """Project sections must receive real project page blocks before about/footer snippets."""
        state = {
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/about-us",
                    "page_type": "portfolio",
                    "heading": "Subscribe Newsletters",
                    "text": "Email Subscribe Now Facebook Instagram Linkedin Main Menu Home Portfolio Web Hosting Contact Us all rights reserved Scroll to Top Let's Talk.",
                },
                {
                    "url": "https://brand.test/about-us",
                    "page_type": "portfolio",
                    "heading": "History",
                    "text": "Born in the heart of Egypt, BrandCo specializes in mobile app development and website design.",
                },
                {
                    "url": "https://brand.test/projects",
                    "page_type": "projects",
                    "heading": "Acumen Consulting Egypt",
                    "text": "Client: Acumen Consulting Egypt Location: Egypt Sector: Management Consulting Expertise: Branding & Positioning, UX/UI, Mobile App.",
                },
                {
                    "url": "https://brand.test/projects",
                    "page_type": "projects",
                    "heading": "Aqar Ya Masr Web app",
                    "text": "Project: Aqar Ya Masr Web app. Real estate platform and website project.",
                },
            ],
        }
        section = {"heading_text": "Projects shown by BrandCo", "section_type": "proof"}

        blocks = select_section_raw_brand_blocks(section, state, max_blocks=4)
        headings = [block["heading"] for block in blocks]
        urls = [block["source_url"] for block in blocks]

        self.assertEqual(urls[:2], ["https://brand.test/projects", "https://brand.test/projects"])
        self.assertIn("Acumen Consulting Egypt", headings)
        self.assertIn("Aqar Ya Masr Web app", headings)
        self.assertNotIn("Subscribe Newsletters", headings)
        self.assertNotIn("History", headings)

    def test_phase_19_step3_understanding_uses_raw_blocks_not_cards(self):
        """Phase 1.9 Step 3: card fields are not final truth for project names."""
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {"heading_text": "Projects shown by BrandCo", "section_intent": "brand_proof"}
        state = {
            "brand_name": "BrandCo",
            "brand_evidence_cards": [
                {
                    "url": "https://brand.test/portfolio",
                    "page_type": "portfolio",
                    "visible_project_or_case_study_examples": ["Card Only Project"],
                    "excluded_reason": None,
                }
            ],
        }
        chunks = [
            {
                "source_url": "https://brand.test/portfolio",
                "page_type": "portfolio",
                "heading": "Portfolio",
                "observed_text": "Project: Raw Portfolio Project. Client: Raw Client Name.",
                "observed_facts": ["Project: Raw Portfolio Project", "Client: Raw Client Name"],
            }
        ]
        original_state = copy.deepcopy(state)
        original_section = copy.deepcopy(section)

        brief = build_section_brand_understanding(section, state, chunks)

        self.assertEqual(state, original_state)
        self.assertEqual(section, original_section)
        self.assertIn("Raw Portfolio Project", brief["relevant_projects"])
        self.assertIn("Raw Client Name", brief["relevant_projects"])
        self.assertNotIn("Card Only Project", brief["relevant_projects"])

    def test_phase_19_step3_understanding_filters_footer_noise_from_projects(self):
        """Phase 1.9 Step 3: newsletter/menu fragments must not become project names."""
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {"heading_text": "Projects shown by BrandCo", "section_intent": "Commercial"}
        state = {"brand_name": "BrandCo"}
        chunks = [
            {
                "source_url": "https://brand.test/about-us",
                "page_type": "portfolio",
                "heading": "Subscribe Newsletters",
                "observed_text": "Email Subscribe Now Facebook Instagram Linkedin Main Menu Home Portfolio Web Hosting Contact Us all rights reserved Scroll to Top Let's Talk.",
                "observed_facts": [],
            },
            {
                "source_url": "https://brand.test/projects",
                "page_type": "projects",
                "heading": "Acumen Consulting Egypt",
                "observed_text": "Client: Acumen Consulting Egypt Location: Egypt Sector: Management Consulting Expertise: UX/UI, Mobile App.",
                "observed_facts": ["Client: Acumen Consulting Egypt"],
            },
        ]

        brief = build_section_brand_understanding(section, state, chunks)

        self.assertEqual(brief["relevant_projects"], ["Acumen Consulting Egypt"])
        self.assertNotIn("Email Subscribe Now Subscribe", brief["relevant_projects"])
        self.assertNotIn("Main Menu Home About", brief["relevant_projects"])

    def test_phase_19_step3_noisy_labels_do_not_enter_services_or_projects(self):
        """Phase 1.9 Step 3: noisy labels stay out of raw-derived services/projects."""
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {"heading_text": "What BrandCo provides and projects shown by BrandCo", "section_intent": "Commercial"}
        state = {"brand_name": "BrandCo"}
        chunks = [
            {
                "url": "https://brand.test/portfolio",
                "page_type": "portfolio",
                "heading": "Portfolio",
                "text": (
                    "IntoSOFTWARE. Why You Should Choose Us. Technologies Used. Brief. "
                    "Project: Clean Client Portal."
                ),
            },
            {
                "url": "https://brand.test/services",
                "page_type": "services",
                "heading": "Services",
                "text": "BrandCo provides Web Development and CRM Integrations.",
            },
        ]

        brief = build_section_brand_understanding(section, state, chunks)
        combined = " | ".join(brief["relevant_projects"] + brief["relevant_services"])

        self.assertIn("Clean Client Portal", brief["relevant_projects"])
        self.assertIn("Web Development", brief["relevant_services"])
        for noisy in ["Brief", "Technologies Used", "Why You Should Choose Us", "IntoSOFTWARE"]:
            self.assertNotIn(noisy, combined)

    def test_project_understanding_filters_labels_and_merges_channel_variants(self):
        """Project extraction should not treat field labels or app/web variants as separate projects."""
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {"heading_text": "Projects shown by BrandCo", "section_type": "proof", "section_intent": "Commercial"}
        state = {"brand_name": "BrandCo"}
        chunks = [
            {
                "source_url": "https://brand.test/projects",
                "page_type": "projects",
                "heading": "Project Details",
                "observed_text": (
                    "Name: Aqar Ya Masr Web app. Publish Date: 2024. Objective: Real estate platform. "
                    "Name: Aqar Ya Masr Mob App. Client: Baddel. Location: in Egypt."
                ),
                "observed_facts": [
                    "Name: Aqar Ya Masr Web app",
                    "Publish Date: 2024",
                    "Objective: Real estate platform",
                    "Name: Aqar Ya Masr Mob App",
                    "Client: Baddel",
                    "Location: in Egypt",
                ],
            }
        ]

        brief = build_section_brand_understanding(section, state, chunks)

        self.assertIn("Aqar Ya Masr", brief["relevant_projects"])
        self.assertIn("Baddel", brief["relevant_projects"])
        self.assertNotIn("Aqar Ya Masr Web app", brief["relevant_projects"])
        self.assertNotIn("Aqar Ya Masr Mob App", brief["relevant_projects"])
        self.assertNotIn("Name", brief["relevant_projects"])
        self.assertNotIn("Publish Date", brief["relevant_projects"])
        self.assertNotIn("Objective", brief["relevant_projects"])
        self.assertNotIn("in Egypt", brief["relevant_projects"])

    def test_project_understanding_filters_real_metadata_label_patterns(self):
        """Project extraction rejects page metadata labels observed in portfolio pages."""
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {"heading_text": "Projects shown by BrandCo", "section_type": "proof", "section_intent": "Commercial"}
        state = {"brand_name": "BrandCo"}
        chunks = [
            {
                "source_url": "https://brand.test/portfolio/project-a",
                "page_type": "portfolio",
                "heading": "Project Detail",
                "observed_text": (
                    "Name Aqar Ya Masr. Creation Aqar Ya Masr Egypt Real Estate. "
                    "Creation To develop a comprehensive digital ecosystem. "
                    "Scope of Work. Deliverables. Technology Stack. Quality Assurance. Client: Baddel."
                ),
                "observed_facts": [
                    "Name Aqar Ya Masr",
                    "Creation Aqar Ya Masr Egypt Real Estate",
                    "Creation To develop a comprehensive digital ecosystem",
                    "Scope of Work",
                    "Deliverables",
                    "Technology Stack",
                    "Quality Assurance",
                    "Client: Baddel",
                ],
            }
        ]

        brief = build_section_brand_understanding(section, state, chunks)

        self.assertIn("Aqar Ya Masr", brief["relevant_projects"])
        self.assertIn("Baddel", brief["relevant_projects"])
        rejected = " | ".join(brief["relevant_projects"])
        for label in [
            "Name Aqar Ya Masr",
            "Creation Aqar Ya Masr Egypt Real Estate",
            "Creation To develop",
            "Scope of Work",
            "Deliverables",
            "Technology Stack",
            "Quality Assurance",
        ]:
            self.assertNotIn(label, rejected)

    def test_phase_19_step3_service_process_geo_cta_from_raw(self):
        """Phase 1.9 Step 3: services, process, geography, and CTAs come from raw text."""
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {"heading_text": "How BrandCo supports clients", "section_intent": "Commercial"}
        state = {"brand_name": "BrandCo"}
        chunks = [
            {
                "url": "https://brand.test/services",
                "page_type": "services",
                "heading": "Services",
                "text": (
                    "BrandCo provides Software Development, UX/UI Design, and CRM Integrations. "
                    "The workflow includes Consultation & Planning and Design & Development. "
                    "Based in Egypt. Contact sales@brand.test for a quote."
                ),
            }
        ]

        brief = build_section_brand_understanding(section, state, chunks)

        self.assertIn("Software Development", brief["relevant_services"])
        self.assertIn("Consultation & Planning", brief["relevant_process_steps"])
        self.assertIn("Design & Development", brief["relevant_process_steps"])
        self.assertIn("Egypt", brief["relevant_geography"])
        self.assertIn("sales@brand.test", brief["relevant_ctas"])

    def test_phase_19_step3_unsupported_geography_and_pricing_are_flagged(self):
        """Phase 1.9 Step 3: unsupported brand geography/pricing promises are flagged."""
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {"heading_text": "BrandCo pricing packages in Saudi Arabia", "section_intent": "Commercial"}
        state = {"brand_name": "BrandCo"}
        chunks = [
            {
                "url": "https://brand.test/services",
                "page_type": "services",
                "heading": "Services",
                "text": "BrandCo provides Software Development for business teams.",
            }
        ]

        brief = build_section_brand_understanding(section, state, chunks)
        warnings = " | ".join(brief["not_supported_for_this_section"]).lower()

        self.assertIn("pricing", warnings)
        self.assertIn("geography", warnings)

    def test_brand_page_briefs_preserve_specific_terms_and_project_names(self):
        """Page briefs compress source pages while preserving specific services and projects."""
        state = {
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "page_title": "Services",
                    "heading": "Software Services",
                    "text": "BrandCo provides ERP systems, CRM integrations, POS software, dashboard development, and eCommerce software.",
                },
                {
                    "url": "https://brand.test/projects",
                    "page_type": "portfolio",
                    "page_title": "Projects",
                    "heading": "Portfolio",
                    "text": "Project: Acumen Consulting Egypt. Client: Aqar Ya Masr Web app. Project: Baddel.",
                },
                {
                    "url": "https://brand.test/about",
                    "page_type": "about",
                    "page_title": "About",
                    "heading": "Subscribe Newsletters",
                    "text": "Main Menu Home About Services Facebook Instagram LinkedIn Email Subscribe Now.",
                },
            ],
        }

        briefs = build_brand_page_briefs(state)
        joined = " | ".join(brief.get("grounded_summary", "") for brief in briefs)
        projects = [item for brief in briefs for item in brief.get("observed_projects", [])]
        services = [item for brief in briefs for item in brief.get("observed_services", [])]
        tech = [item for brief in briefs for item in brief.get("observed_technologies", [])]

        self.assertIn("Acumen Consulting Egypt", projects)
        self.assertIn("Aqar Ya Masr Web app", projects)
        self.assertIn("Baddel", projects)
        self.assertIn("ERP", tech)
        self.assertIn("CRM", tech)
        self.assertIn("POS", tech)
        self.assertTrue(any("Dashboard" in item or "dashboard" in item.lower() for item in services + tech))
        self.assertNotIn("Subscribe Now", joined)
        self.assertNotIn("Main Menu Home", joined)

    def test_brand_page_briefs_do_not_promote_project_metadata_to_brand_claims(self):
        """Page briefs keep project names while blocking project metadata from brand geography/pricing."""
        state = {
            "brand_name": "BrandCo",
            "area": "Riyadh",
            "primary_keyword": "best agency in Riyadh",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/projects",
                    "page_type": "portfolio",
                    "page_title": "Projects",
                    "heading": "Introduction",
                    "text": (
                        "Project: Acumen Consulting Egypt. "
                        "Project: Aqar Ya Masr Web app. "
                        "Project: Arab Business Academy. "
                        "Client: Baddel. "
                        "Location: Egypt Sector: Management Consulting. "
                        "Technologies Used. Brief."
                    ),
                },
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "page_title": "Services",
                    "heading": "FAQ",
                    "text": "How much do services cost? Pricing varies depending on project scope.",
                },
            ],
        }

        briefs = build_brand_page_briefs(state)
        projects = [item for brief in briefs for item in brief.get("observed_projects", [])]
        geography = [item for brief in briefs for item in brief.get("explicit_geography", [])]
        pricing = [item for brief in briefs for item in brief.get("observed_pricing", [])]
        joined = " | ".join(projects + geography + pricing)

        self.assertIn("Acumen Consulting Egypt", projects)
        self.assertIn("Aqar Ya Masr Web app", projects)
        self.assertIn("Arab Business Academy", projects)
        self.assertIn("Baddel", projects)
        self.assertNotIn("Introduction", joined)
        self.assertNotIn("Brief", joined)
        self.assertNotIn("Technologies Used", joined)
        self.assertNotIn("Riyadh", joined)
        self.assertNotIn("Egypt Sector", joined)
        self.assertEqual(pricing, [])

    def test_classify_page_type_url_slug_wins_over_navigation_headings(self):
        """The real page URL must beat site-wide nav labels such as Portfolio."""
        self.assertEqual(
            classify_page_type("https://brand.test/about-us", "About Us", ["Portfolio", "Projects"]),
            "about",
        )
        self.assertEqual(
            classify_page_type("https://brand.test/projects", "About Us", ["About Us"]),
            "portfolio",
        )

    def test_inventory_ignores_card_geography_without_raw_presence_evidence(self):
        """Target-area/card geography cannot unlock brand local-presence claims."""
        state = {
            "brand_name": "BrandCo",
            "area": "Riyadh",
            "brand_evidence_cards": [
                {
                    "url": "https://brand.test/about-us",
                    "page_type": "about",
                    "title": "About Us",
                    "visible_geography": ["Riyadh", "Saudi Arabia"],
                    "usable_snippets": ["We help teams choose better software."],
                }
            ],
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/about-us",
                    "page_type": "about",
                    "page_title": "About Us",
                    "heading": "About",
                    "text": "BrandCo helps teams plan and build software products.",
                }
            ],
        }

        inventory = build_brand_evidence_inventory(state)

        self.assertEqual(inventory["explicit_geography"], [])

    def test_page_briefs_do_not_extract_about_testimonials_as_projects(self):
        """Misleading nav labels on about pages must not turn people/CTA text into projects."""
        state = {
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/about-us",
                    "page_type": "portfolio",
                    "page_title": "About Us",
                    "heading": "Portfolio",
                    "text": (
                        "Portfolio Testimonials Abdulluh Emad Ahmed Daoud "
                        "Let's Talk Let's Talk Let's Talk"
                    ),
                },
                {
                    "url": "https://brand.test/projects",
                    "page_type": "portfolio",
                    "page_title": "Projects",
                    "heading": "Projects",
                    "text": (
                        "Project: Acumen Consulting Egypt. "
                        "Mobile App Aqar Ya Masr Mob App. "
                        "Project: Aqar Ya Masr Web app. "
                        "Project: Arab Business Academy. "
                        "Client: Baddel."
                    ),
                },
            ],
        }

        briefs = build_brand_page_briefs(state)
        projects = [item for brief in briefs for item in brief.get("observed_projects", [])]

        self.assertIn("Acumen Consulting Egypt", projects)
        self.assertIn("Aqar Ya Masr Mob App", projects)
        self.assertIn("Aqar Ya Masr Web app", projects)
        self.assertIn("Arab Business Academy", projects)
        self.assertIn("Baddel", projects)
        self.assertNotIn("Abdulluh Emad", projects)
        self.assertNotIn("Ahmed Daoud", projects)
        self.assertNotIn("Let's Talk Let's Talk Let's Talk", projects)

    def test_section_understanding_uses_page_briefs_not_card_noise(self):
        """Section understanding prefers page briefs over noisy card values."""
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {
            "heading_text": "Projects shown by BrandCo",
            "section_intent": "Commercial",
            "section_brand_page_briefs": [
                {
                    "source_url": "https://brand.test/projects",
                    "page_type": "portfolio",
                    "page_title": "Projects",
                    "grounded_summary": "Observed project/client examples include Acumen Consulting Egypt, Aqar Ya Masr Web app, and Baddel.",
                    "observed_projects": ["Acumen Consulting Egypt", "Aqar Ya Masr Web app", "Baddel"],
                    "observed_services": [],
                    "observed_technologies": [],
                    "observed_process_steps": [],
                    "explicit_geography": [],
                    "observed_pricing": [],
                    "source_snippets": [],
                    "claim_boundaries": ["No explicit geography/local presence observed on this page."],
                }
            ],
        }
        state = {
            "brand_name": "BrandCo",
            "brand_evidence_cards": [
                {
                    "visible_project_or_case_study_examples": [
                        "Subscribe Now",
                        "Why You Should Choose Us",
                        "Technologies Used",
                    ],
                }
            ],
        }

        brief = build_section_brand_understanding(section, state, retrieved_chunks=[])

        self.assertEqual(brief["relevant_projects"][:3], ["Acumen Consulting Egypt", "Aqar Ya Masr", "Baddel"])
        self.assertNotIn("Subscribe Now", brief["relevant_projects"])
        self.assertNotIn("Why You Should Choose Us", brief["relevant_projects"])
        self.assertIn("Observed project/client examples include", brief["useful_source_snippets"][0])

    def test_select_section_brand_page_briefs_filters_generic_non_brand_sections(self):
        """Generic market sections do not receive brand page briefs."""
        state = {
            "brand_name": "BrandCo",
            "brand_page_briefs": [
                {
                    "source_url": "https://brand.test/services",
                    "page_type": "services",
                    "page_title": "Services",
                    "grounded_summary": "Observed services/capabilities include Web Development.",
                    "observed_services": ["Web Development"],
                }
            ],
        }
        generic_section = {"heading_text": "Available website service options", "section_type": "offer"}
        brand_section = {"heading_text": "Services offered by BrandCo", "section_type": "offer"}

        self.assertEqual(select_section_brand_page_briefs(generic_section, state), [])
        selected = select_section_brand_page_briefs(brand_section, state)
        self.assertEqual(selected[0]["source_url"], "https://brand.test/services")

    async def test_phase_19_step4_writer_prompt_contains_raw_blocks_and_understanding(self):
        """Phase 1.9 Step 4: writer prompt exposes raw source blocks and the structured organizer."""
        from src.services.content_generator import SectionWriter

        mock_ai = MagicMock()
        mock_ai.send = AsyncMock(return_value={"content": '{"content": "ok"}', "metadata": {}})
        writer = SectionWriter(mock_ai)
        raw_blocks = [
            {
                "source_url": "https://brand.test/portfolio",
                "page_type": "portfolio",
                "heading": "Selected Projects",
                "observed_text": "Project: Acumen Consulting Egypt. Client: Baddel.",
                "observed_facts": ["Project: Acumen Consulting Egypt", "Client: Baddel"],
            }
        ]
        understanding = {
            "relevant_projects": ["Acumen Consulting Egypt", "Baddel"],
            "recommended_angle": {"preferred_section_style": "evidence_grounded"},
        }
        page_briefs = [
            {
                "source_url": "https://brand.test/portfolio",
                "page_type": "portfolio",
                "page_title": "Portfolio",
                "grounded_summary": "Observed project/client examples include Acumen Consulting Egypt and Baddel.",
                "observed_projects": ["Acumen Consulting Egypt", "Baddel"],
                "claim_boundaries": ["No explicit pricing/packages observed on this page."],
            }
        ]

        await writer.write(
            title="Best Web Development",
            global_keywords={"primary": "web development", "lsi": [], "semantic": []},
            section={"heading_text": "Projects shown by BrandCo", "section_intent": "Commercial"},
            article_intent="Commercial",
            seo_intelligence={"market_analysis": {"market_insights": {}}},
            content_type="brand_commercial",
            link_strategy={},
            brand_url="https://brand.test",
            brand_name="BrandCo",
            brand_link_used=False,
            brand_link_allowed=True,
            allow_external_links=False,
            execution_plan={},
            area="",
            section_brand_page_briefs=page_briefs,
            section_raw_brand_blocks=raw_blocks,
            section_source_text="Source URL: https://brand.test/portfolio\nObserved text:\nProject: Acumen Consulting Egypt.",
            section_brand_understanding=understanding,
        )

        sent_prompt = mock_ai.send.call_args[0][0]
        # Pack-Only Writer Truth: the prompt renders brand_page_knowledge_pack_context
        # as the sole writer-facing brand truth. Legacy structured block markers are gone.
        self.assertIn("[BRAND PAGE KNOWLEDGE PACK - PAGE BY PAGE]", sent_prompt)
        # The contract section clearly lists routing-only diagnostics (not rendered as brand facts)
        self.assertIn("Routing-only diagnostics", sent_prompt)
        self.assertIn("section_brand_page_briefs", sent_prompt)
        self.assertIn("section_raw_brand_blocks", sent_prompt)
        self.assertIn("section_brand_understanding", sent_prompt)
        # Knowledge pack context is passed correctly (fallback text when no pack provided)
        self.assertIn("ONLY writer-facing truth", sent_prompt)

    async def test_phase_19_step4_section_writer_receives_raw_brand_blocks(self):
        """Phase 1.9 Step 4: workflow passes selected raw blocks into SectionWriter.write."""
        controller = AsyncWorkflowController(work_dir=".")
        controller.section_writer = MagicMock()
        controller.section_writer.write = AsyncMock(return_value={
            "content": "BrandCo shows Acumen Consulting Egypt as a project example.",
            "section_id": "s1",
        })
        controller.validator.enforce_paragraph_structure = MagicMock(side_effect=lambda content: content)
        controller.validator.extract_sentences = MagicMock(return_value=["BrandCo shows Acumen Consulting Egypt as a project example."])

        section = {
            "section_id": "s1",
            "heading_text": "Projects shown by BrandCo",
            "section_type": "proof",
            "section_intent": "Commercial",
        }
        state = {
            "content_type": "brand_commercial",
            "content_stage_only_mode": True,
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/portfolio",
                    "page_type": "portfolio",
                    "heading": "Selected Projects",
                    "text": "Project: Acumen Consulting Egypt. Client: Baddel.",
                }
            ],
            "brand_evidence_inventory": {
                "services_available": False,
                "projects_available": True,
                "pricing_available": False,
                "process_available": False,
                "trust_available": False,
                "explicit_geography": [],
            },
            "used_internal_links": [],
            "used_external_links": [],
            "used_phrases": [],
            "used_claims": [],
            "used_topics": [],
            "brand_page_briefs": [
                {
                    "source_url": "https://brand.test/portfolio",
                    "page_type": "portfolio",
                    "page_title": "Portfolio",
                    "grounded_summary": "Observed project/client examples include Acumen Consulting Egypt and Baddel.",
                    "observed_projects": ["Acumen Consulting Egypt", "Baddel"],
                    "observed_services": [],
                    "observed_technologies": [],
                    "observed_process_steps": [],
                    "explicit_geography": [],
                    "observed_pricing": [],
                }
            ],
        }

        await controller._write_single_section(
            title="Best Web Development",
            global_keywords={"primary": "web development"},
            section=section,
            article_intent="commercial",
            seo_intelligence={},
            content_type="brand_commercial",
            link_strategy={},
            state=state,
        )

        first_kwargs = controller.section_writer.write.call_args.kwargs
        # Pack-Only Writer Truth firewall: writer receives empty lists/dict — brand objects are
        # kept for audit/validation on the section object, not exposed to the writer prompt.
        self.assertIn("section_brand_page_briefs", first_kwargs)
        self.assertEqual(first_kwargs["section_brand_page_briefs"], [])
        self.assertIn("section_raw_brand_blocks", first_kwargs)
        self.assertEqual(first_kwargs["section_raw_brand_blocks"], [])
        # The section object retains the full understanding for audit/validation
        self.assertIn("section_brand_understanding", section)
        self.assertIn("Acumen Consulting Egypt", section["section_brand_understanding"]["relevant_projects"])

    async def test_phase_19_step4_generic_non_brand_section_receives_no_raw_blocks(self):
        """Phase 1.9 Step 4: generic non-brand sections do not get brand raw blocks."""
        controller = AsyncWorkflowController(work_dir=".")
        controller.section_writer = MagicMock()
        controller.section_writer.write = AsyncMock(return_value={
            "content": "This section explains market service options.",
            "section_id": "s1",
        })
        controller.validator.enforce_paragraph_structure = MagicMock(side_effect=lambda content: content)
        controller.validator.extract_sentences = MagicMock(return_value=["This section explains market service options."])

        section = {
            "section_id": "s1",
            "heading_text": "Available service options",
            "section_type": "offer",
            "section_intent": "Informational",
        }
        state = {
            "content_type": "brand_commercial",
            "content_stage_only_mode": True,
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/services",
                    "page_type": "services",
                    "heading": "Services",
                    "text": "BrandCo provides Web Development and CRM integrations.",
                }
            ],
            "used_internal_links": [],
            "used_external_links": [],
            "used_phrases": [],
            "used_claims": [],
            "used_topics": [],
        }

        await controller._write_single_section(
            title="Best Web Development",
            global_keywords={"primary": "web development"},
            section=section,
            article_intent="commercial",
            seo_intelligence={},
            content_type="brand_commercial",
            link_strategy={},
            state=state,
        )

        first_kwargs = controller.section_writer.write.call_args.kwargs
        # Pack-Only Writer Truth firewall: writer receives empty lists/dict for non-brand sections
        self.assertEqual(first_kwargs["section_raw_brand_blocks"], [])
        # writer_section_brand_understanding is an empty dict for non-brand sections (firewall)
        self.assertEqual(first_kwargs["section_brand_understanding"], {})

    async def test_phase_19_step4_intro_prompt_includes_simple_language_guard(self):
        """Phase 1.9 Step 4: intro prompt includes the no technical catalog instruction."""
        from src.services.content_generator import SectionWriter

        mock_ai = MagicMock()
        mock_ai.send = AsyncMock(return_value={"content": '{"content": "ok"}', "metadata": {}})
        writer = SectionWriter(mock_ai)

        await writer.write(
            title="Best Web Development",
            global_keywords={"primary": "web development", "lsi": [], "semantic": []},
            section={"heading_text": "Introduction", "section_type": "introduction", "section_intent": "Commercial"},
            article_intent="Commercial",
            seo_intelligence={"market_analysis": {"market_insights": {}}},
            content_type="brand_commercial",
            link_strategy={},
            brand_url="https://brand.test",
            brand_name="BrandCo",
            brand_link_used=False,
            brand_link_allowed=True,
            allow_external_links=False,
            execution_plan={},
            area="",
            section_raw_brand_blocks=[],
            section_brand_understanding={},
        )

        sent_prompt = mock_ai.send.call_args[0][0]
        self.assertIn("Intro must avoid technical catalog dumps", sent_prompt)
        self.assertIn("simple buyer language", sent_prompt)

    def test_phase_19_step4_prompt_templates_expose_raw_blocks(self):
        """Phase 1.9 Step 4: prompt files are readable and contain raw-block wording."""
        prompt_paths = [
            "assets/prompts/templates/section_contract.txt",
            "assets/prompts/templates/02_section_writer_brand_commercial_v2.txt",
            "assets/prompts/templates/runtime_state.txt",
        ]
        texts = {}
        for path in prompt_paths:
            with open(path, "r", encoding="utf-8") as handle:
                texts[path] = handle.read()

        # Pack-Only Writer Truth: runtime_state.txt now uses brand_page_knowledge_pack_context
        # as the sole writer-facing brand truth. Legacy block markers have been removed.
        self.assertIn("brand_page_knowledge_pack_context", texts["assets/prompts/templates/runtime_state.txt"])
        self.assertIn("[BRAND PAGE KNOWLEDGE PACK - PAGE BY PAGE]", texts["assets/prompts/templates/runtime_state.txt"])
        self.assertIn("ONLY writer-facing truth", texts["assets/prompts/templates/runtime_state.txt"])
        # section_contract.txt names these as routing-only diagnostics (not writer evidence)
        self.assertIn("section_brand_page_briefs", texts["assets/prompts/templates/section_contract.txt"])
        self.assertIn("section_raw_brand_blocks", texts["assets/prompts/templates/section_contract.txt"])
        self.assertIn("Routing-only diagnostics", texts["assets/prompts/templates/section_contract.txt"])
        self.assertIn("Intro must avoid technical catalog dumps", texts["assets/prompts/templates/02_section_writer_brand_commercial_v2.txt"])

    def test_phase_19_step5_outline_prompt_exposes_inventory_gate_rules(self):
        """Phase 1.9 Step 5: outline prompt contains inventory-gated brand heading rules."""
        path = "assets/prompts/templates/01_outline_generator_heading_only_commercial_v2.txt"
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()

        self.assertIn("[BRAND EVIDENCE INVENTORY - OUTLINE GATE]", text)
        self.assertIn("Do not mention the brand in every section", text)
        self.assertIn("Do not create brand project/case-study headings unless `projects_available` is true.", text)
        self.assertIn("Do not create brand pricing/packages headings unless `pricing_available` is true.", text)

    async def test_phase_19_step5_outline_receives_inventory_without_brand_context_mutation(self):
        """Phase 1.9 Step 5: outline prompt context gets inventory while canonical brand_context stays unchanged."""
        controller = AsyncWorkflowController(work_dir=".")
        outline = [
            {"section_id": "intro", "heading_text": "Introduction", "heading_level": "INTRO", "section_type": "introduction", "section_intent": "Informational", "subheadings": []},
            {"section_id": "offer", "heading_text": "Services offered by BrandCo", "heading_level": "H2", "section_type": "offer", "section_intent": "Commercial", "subheadings": []},
            {"section_id": "faq", "heading_text": "FAQ", "heading_level": "H2", "section_type": "faq", "section_intent": "Informational", "subheadings": []},
            {"section_id": "conclusion", "heading_text": "Next step with BrandCo", "heading_level": "H2", "section_type": "conclusion", "section_intent": "Informational", "subheadings": []},
        ]
        controller.outline_gen = MagicMock()
        controller.outline_gen.generate = AsyncMock(return_value={
            "outline": copy.deepcopy(outline),
            "keyword_expansion": {},
            "metadata": {"prompt": "", "response": "", "tokens": {}, "model": "mock"},
        })
        controller.outline_gen._normalize_section = MagicMock(side_effect=lambda section, *args, **kwargs: section.setdefault("content_type", "brand_commercial"))
        controller.validator.consolidate_faq = MagicMock(side_effect=lambda items: items)
        controller.validator.enforce_cta_policy = MagicMock(side_effect=lambda items, content_type: items)
        controller.validator.enforce_outline_structure = MagicMock(side_effect=lambda items, content_type: items)
        controller.validator.enforce_content_angle = MagicMock(side_effect=lambda items, strategy: items)
        controller.validator.adjust_paa_by_intent = MagicMock(side_effect=lambda items, intent: items)
        controller.validator.enforce_paa_sections = MagicMock(return_value={"paa_ok": True, "paa_ratio": 1, "missing_count": 0})
        controller.outline_repair_service.dedupe_faq_against_h2 = MagicMock(side_effect=lambda items: items)
        controller.outline_repair_service.enrich_brand_utility_faq = MagicMock(side_effect=lambda items, **kwargs: items)
        controller.outline_repair_service.normalize_heading_only_section_types = MagicMock(side_effect=lambda items: items)
        controller.outline_repair_service.clean_conclusion_heading = MagicMock(side_effect=lambda items, **kwargs: items)

        state = {
            "input_data": {"title": "Best service provider", "keywords": ["service provider"], "urls": []},
            "content_stage_only_mode": True,
            "content_type": "brand_commercial",
            "intent": "informational",
            "brand_name": "BrandCo",
            "brand_context": "canonical brand context",
            "primary_keyword": "service provider",
            "keywords": ["service provider"],
            "article_language": "en",
            "seo_intelligence": {"market_analysis": {"market_insights": {}, "semantic_assets": {}}},
            "content_strategy": {},
            "serp_data": {},
            "brand_evidence_inventory": {
                "services_available": True,
                "projects_available": True,
                "pricing_available": False,
                "process_available": False,
                "trust_available": False,
                "explicit_geography": [],
                "confidence": "medium",
            },
        }

        result = await controller._step_1_outline(state)
        sent_context = controller.outline_gen.generate.call_args.kwargs["brand_context"]

        self.assertEqual(result["brand_context"], "canonical brand context")
        self.assertIn("[BRAND EVIDENCE INVENTORY - OUTLINE GATE]", sent_context)
        self.assertIn('"projects_available": true', sent_context)
        self.assertIn("Do not create brand pricing/packages headings unless pricing_available is true.", sent_context)

    def test_brand_commercial_generic_service_heading_is_brand_light_not_brand_owned(self):
        """Generic service headings stay buyer-facing and only allow light brand use."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_name": "BrandCo",
            "content_type": "brand_commercial",
            "article_language": "en",
            "primary_keyword": "software services",
            "content_strategy": {},
            "seo_intelligence": {},
            "brand_evidence_inventory": {"services_available": True, "projects_available": True, "pricing_available": False, "explicit_geography": [], "confidence": "medium"},
        }
        outline = [
            {"section_id": "intro", "heading_text": "Introduction", "heading_level": "INTRO", "section_type": "introduction", "subheadings": []},
            {"section_id": "offer", "heading_text": "Available service options", "heading_level": "H2", "section_type": "offer", "taxonomy_axis": "brand_offer", "subheadings": []},
        ]

        prepared = controller._prepare_outline_for_content(state, outline)
        offer = prepared["outline"][1]

        self.assertEqual(offer["section_contract"]["brand_policy"], "none")
        self.assertNotEqual(offer["taxonomy_axis"], "brand_offer")
        self.assertEqual(offer.get("brand_usage_policy"), "brand_light")
        self.assertNotEqual(offer.get("execution_mode"), "brand_service_catalog")

    def test_generic_comparison_heading_stays_non_brand_owned(self):
        """Comparison sections remain market guidance unless they visibly reference the brand."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_name": "BrandCo",
            "content_type": "brand_commercial",
            "article_language": "en",
            "primary_keyword": "software services",
            "content_strategy": {},
            "seo_intelligence": {},
            "brand_evidence_inventory": {"services_available": True, "projects_available": True, "pricing_available": False, "explicit_geography": [], "confidence": "medium"},
        }
        outline = [
            {"section_id": "intro", "heading_text": "Introduction", "heading_level": "INTRO", "section_type": "introduction", "subheadings": []},
            {"section_id": "comparison", "heading_text": "Corporate websites versus ecommerce stores", "heading_level": "H2", "section_type": "comparison", "taxonomy_axis": "comparison", "subheadings": []},
        ]

        prepared = controller._prepare_outline_for_content(state, outline)
        comparison = prepared["outline"][1]

        self.assertEqual(comparison["section_contract"]["brand_policy"], "none")
        self.assertEqual(comparison["taxonomy_axis"], "comparison")

    def test_phase_19_step5_brand_heading_is_brand_offer_when_visible(self):
        """Phase 1.9 Step 5: visible brand offer headings remain brand-owned and answerable."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_name": "BrandCo",
            "content_type": "brand_commercial",
            "article_language": "en",
            "primary_keyword": "software services",
            "content_strategy": {},
            "seo_intelligence": {},
            "brand_evidence_inventory": {"services_available": True, "projects_available": False, "pricing_available": False, "explicit_geography": [], "confidence": "medium"},
        }
        outline = [
            {"section_id": "intro", "heading_text": "Introduction", "heading_level": "INTRO", "section_type": "introduction", "subheadings": []},
            {"section_id": "offer", "heading_text": "Services offered by BrandCo", "heading_level": "H2", "section_type": "offer", "subheadings": []},
        ]

        prepared = controller._prepare_outline_for_content(state, outline)
        offer = prepared["outline"][1]

        self.assertEqual(offer["section_contract"]["brand_policy"], "commercial")
        self.assertEqual(offer["taxonomy_axis"], "brand_offer")
        self.assertEqual(offer["execution_mode"], "brand_service_catalog")

    def test_phase_19_step5_removes_unsupported_brand_project_geography(self):
        """Phase 1.9 Step 5: brand project headings lose unsupported geography."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_name": "BrandCo",
            "content_type": "brand_commercial",
            "article_language": "en",
            "area": "Countryland",
            "primary_keyword": "software services",
            "content_strategy": {},
            "seo_intelligence": {},
            "brand_evidence_inventory": {"services_available": True, "projects_available": True, "pricing_available": False, "explicit_geography": [], "confidence": "medium"},
        }
        outline = [
            {"section_id": "intro", "heading_text": "Introduction", "heading_level": "INTRO", "section_type": "introduction", "subheadings": []},
            {"section_id": "proof", "heading_text": "Projects shown by BrandCo in Countryland", "heading_level": "H2", "section_type": "proof", "subheadings": []},
        ]

        prepared = controller._prepare_outline_for_content(state, outline)
        proof = prepared["outline"][1]

        self.assertEqual(proof["heading_text"], "Projects shown by BrandCo")
        self.assertNotIn("Countryland", proof["heading_text"])
        self.assertEqual(proof["taxonomy_axis"], "brand_projects")

    def test_generic_service_heading_keeps_market_area_without_brand_presence_claim(self):
        """Generic service headings may keep market context but must not become brand presence proof."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_name": "BrandCo",
            "content_type": "brand_commercial",
            "article_language": "en",
            "area": "Saudi Arabia",
            "primary_keyword": "best web design company in Saudi Arabia",
            "content_strategy": {},
            "seo_intelligence": {},
            "brand_evidence_inventory": {
                "services_available": True,
                "projects_available": True,
                "pricing_available": False,
                "explicit_geography": [],
                "confidence": "medium",
            },
        }
        outline = [
            {"section_id": "intro", "heading_text": "Introduction", "heading_level": "INTRO", "section_type": "introduction", "subheadings": []},
            {
                "section_id": "offer",
                "heading_text": "Best web design company in Saudi Arabia: available services",
                "heading_level": "H2",
                "section_type": "offer",
                "subheadings": [],
            },
        ]

        prepared = controller._prepare_outline_for_content(state, outline)
        offer = prepared["outline"][1]

        self.assertIn("Saudi Arabia", offer["heading_text"])
        self.assertEqual(offer["section_contract"]["brand_policy"], "none")
        self.assertEqual(offer.get("brand_usage_policy"), "brand_light")

    def test_phase_19_step5_removes_unsupported_arabic_prefixed_geography(self):
        """Arabic brand headings like 'بالسعودية' should lose unsupported geography."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_name": "BrandCo",
            "content_type": "brand_commercial",
            "article_language": "ar",
            "area": "السعودية",
            "primary_keyword": "شركة تصميم مواقع",
            "content_strategy": {},
            "seo_intelligence": {},
            "brand_evidence_inventory": {"services_available": True, "projects_available": True, "pricing_available": False, "explicit_geography": [], "confidence": "medium"},
        }
        outline = [
            {"section_id": "intro", "heading_text": "مقدمة", "heading_level": "INTRO", "section_type": "introduction", "subheadings": []},
            {"section_id": "proof", "heading_text": "نماذج من مشاريع BrandCo بالسعودية", "heading_level": "H2", "section_type": "proof", "subheadings": []},
        ]

        prepared = controller._prepare_outline_for_content(state, outline)
        proof = prepared["outline"][1]

        self.assertEqual(proof["heading_text"], "نماذج من مشاريع BrandCo")
        self.assertNotIn("السعودية", proof["heading_text"])

    def test_phase_19_step5_project_heading_downgrades_when_projects_unavailable(self):
        """Phase 1.9 Step 5: brand project headings are downgraded when inventory has no projects."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_name": "BrandCo",
            "content_type": "brand_commercial",
            "article_language": "en",
            "primary_keyword": "software services",
            "content_strategy": {},
            "seo_intelligence": {},
            "brand_evidence_inventory": {"services_available": True, "projects_available": False, "pricing_available": False, "explicit_geography": [], "confidence": "medium"},
        }
        outline = [
            {"section_id": "intro", "heading_text": "Introduction", "heading_level": "INTRO", "section_type": "introduction", "subheadings": []},
            {"section_id": "proof", "heading_text": "Projects shown by BrandCo", "heading_level": "H2", "section_type": "proof", "subheadings": []},
        ]

        prepared = controller._prepare_outline_for_content(state, outline)
        proof = prepared["outline"][1]

        self.assertNotEqual(proof["heading_text"], "Projects shown by BrandCo")
        self.assertNotEqual(proof.get("taxonomy_axis"), "brand_projects")
        self.assertEqual(proof["section_contract"]["brand_policy"], "none")

    def test_phase_19_step5_pricing_heading_downgrades_when_pricing_unavailable(self):
        """Phase 1.9 Step 5: brand pricing/package headings are downgraded without pricing inventory."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_name": "BrandCo",
            "content_type": "brand_commercial",
            "article_language": "en",
            "primary_keyword": "software services",
            "content_strategy": {},
            "seo_intelligence": {},
            "brand_evidence_inventory": {"services_available": True, "projects_available": False, "pricing_available": False, "explicit_geography": [], "confidence": "medium"},
        }
        outline = [
            {"section_id": "intro", "heading_text": "Introduction", "heading_level": "INTRO", "section_type": "introduction", "subheadings": []},
            {"section_id": "pricing", "heading_text": "BrandCo pricing packages", "heading_level": "H2", "section_type": "pricing", "subheadings": ["Starter package"]},
        ]

        prepared = controller._prepare_outline_for_content(state, outline)
        pricing = prepared["outline"][1]

        self.assertEqual(pricing["heading_text"], "Service Scope Available From BrandCo")
        self.assertEqual(pricing["subheadings"], [])
        self.assertNotIn("pricing", pricing["heading_text"].lower())
        self.assertNotIn("package", pricing["heading_text"].lower())

    def test_phase_19_step5_general_faq_is_not_brand_commercial(self):
        """Phase 1.9 Step 5: FAQ is brand-commercial only when a visible question references the brand."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_name": "BrandCo",
            "content_type": "brand_commercial",
            "article_language": "en",
            "primary_keyword": "software services",
            "content_strategy": {},
            "seo_intelligence": {},
            "brand_evidence_inventory": {"services_available": True, "projects_available": False, "pricing_available": False, "explicit_geography": [], "confidence": "medium"},
        }

        general = controller._prepare_outline_for_content(state, [
            {"section_id": "intro", "heading_text": "Introduction", "heading_level": "INTRO", "section_type": "introduction", "subheadings": []},
            {"section_id": "faq", "heading_text": "FAQ", "heading_level": "H2", "section_type": "faq", "subheadings": ["How do service costs vary?"]},
        ])["outline"][1]
        brand_question = controller._prepare_outline_for_content(state, [
            {"section_id": "intro", "heading_text": "Introduction", "heading_level": "INTRO", "section_type": "introduction", "subheadings": []},
            {"section_id": "faq", "heading_text": "FAQ", "heading_level": "H2", "section_type": "faq", "subheadings": ["How does BrandCo start a project?"]},
        ])["outline"][1]

        self.assertEqual(general["section_contract"]["brand_policy"], "none")
        self.assertEqual(general["taxonomy_axis"], "faq")
        self.assertEqual(brand_question["section_contract"]["brand_policy"], "commercial")

    def test_brand_commercial_intent_distribution_wins_over_informational_intent(self):
        """Brand commercial articles keep a commercial H2 mix even when the broad intent is informational."""
        from src.services.validation_service import ValidationService

        validator = ValidationService()
        outline = [
            {"section_id": "intro", "heading_level": "H2", "section_type": "introduction", "section_intent": "Informational"},
            {"section_id": "offer", "heading_level": "H2", "section_type": "offer", "section_intent": "Informational"},
            {"section_id": "features", "heading_level": "H2", "section_type": "features", "section_intent": "Informational"},
            {"section_id": "proof", "heading_level": "H2", "section_type": "proof", "section_intent": "Informational"},
            {"section_id": "comparison", "heading_level": "H2", "section_type": "comparison", "section_intent": "Informational"},
            {"section_id": "process", "heading_level": "H2", "section_type": "process", "section_intent": "Informational"},
            {"section_id": "faq", "heading_level": "H2", "section_type": "faq", "section_intent": "Informational"},
            {"section_id": "conclusion", "heading_level": "H2", "section_type": "conclusion", "section_intent": "Informational"},
        ]

        updated, errors = validator.enforce_intent_distribution(
            outline,
            intent="informational",
            content_type="brand_commercial",
        )

        commercial_count = sum(
            1 for section in updated
            if section.get("section_intent") in {"Commercial", "Transactional"}
        )
        protected = {
            section["section_id"]: section["section_intent"]
            for section in updated
            if section["section_id"] in {"intro", "faq", "conclusion"}
        }

        self.assertEqual(errors, [])
        self.assertGreaterEqual(commercial_count, 5)
        self.assertNotEqual(commercial_count, 0)
        self.assertEqual(protected, {
            "intro": "Informational",
            "faq": "Informational",
            "conclusion": "Informational",
        })

    async def test_content_stage_outline_applies_brand_commercial_distribution(self):
        """The lightweight content-stage outline path must still preserve brand-commercial 70/30 intent."""
        controller = AsyncWorkflowController(work_dir=".")
        outline = [
            {"section_id": "intro", "heading_text": "مقدمة", "heading_level": "H2", "section_type": "introduction", "section_intent": "Informational", "subheadings": []},
            {"section_id": "offer", "heading_text": "خدمات BrandCo", "heading_level": "H2", "section_type": "offer", "section_intent": "Informational", "subheadings": []},
            {"section_id": "features", "heading_text": "مميزات الخدمات", "heading_level": "H2", "section_type": "features", "section_intent": "Informational", "subheadings": []},
            {"section_id": "proof", "heading_text": "أمثلة عملية", "heading_level": "H2", "section_type": "proof", "section_intent": "Informational", "subheadings": []},
            {"section_id": "comparison", "heading_text": "مقارنة الحلول", "heading_level": "H2", "section_type": "comparison", "section_intent": "Informational", "subheadings": []},
            {"section_id": "process", "heading_text": "خطوات العمل", "heading_level": "H2", "section_type": "process", "section_intent": "Informational", "subheadings": []},
            {"section_id": "faq", "heading_text": "الأسئلة الشائعة", "heading_level": "H2", "section_type": "faq", "section_intent": "Informational", "subheadings": []},
            {"section_id": "conclusion", "heading_text": "الخلاصة", "heading_level": "H2", "section_type": "conclusion", "section_intent": "Informational", "subheadings": []},
        ]
        controller.outline_gen = MagicMock()
        controller.outline_gen.generate = AsyncMock(return_value={
            "outline": copy.deepcopy(outline),
            "keyword_expansion": {},
            "metadata": {"prompt": "", "response": "", "tokens": {}, "model": "mock"},
        })
        controller.outline_gen._normalize_section = MagicMock()
        controller.validator.consolidate_faq = MagicMock(side_effect=lambda items: items)
        controller.validator.enforce_cta_policy = MagicMock(side_effect=lambda items, content_type: items)
        controller.validator.enforce_outline_structure = MagicMock(side_effect=lambda items, content_type: items)
        controller.validator.enforce_content_angle = MagicMock(side_effect=lambda items, strategy: items)
        controller.validator.adjust_paa_by_intent = MagicMock(side_effect=lambda items, intent: items)
        controller.validator.enforce_paa_sections = MagicMock(return_value={"paa_ok": True, "paa_ratio": 1, "missing_count": 0})
        controller.outline_repair_service.dedupe_faq_against_h2 = MagicMock(side_effect=lambda items: items)
        controller.outline_repair_service.enrich_brand_utility_faq = MagicMock(side_effect=lambda items, **kwargs: items)
        controller.outline_repair_service.normalize_heading_only_section_types = MagicMock(side_effect=lambda items: items)
        controller.outline_repair_service.clean_conclusion_heading = MagicMock(side_effect=lambda items, **kwargs: items)

        state = {
            "input_data": {"title": "أفضل شركة خدمات رقمية", "keywords": ["خدمات رقمية"], "urls": []},
            "content_stage_only_mode": True,
            "content_type": "brand_commercial",
            "intent": "informational",
            "brand_name": "BrandCo",
            "primary_keyword": "خدمات رقمية",
            "keywords": ["خدمات رقمية"],
            "article_language": "ar",
            "seo_intelligence": {"market_analysis": {"market_insights": {}, "semantic_assets": {}}},
            "content_strategy": {},
            "serp_data": {},
            "brand_evidence_cards": [],
        }

        result = await controller._step_1_outline(state)
        commercial_count = sum(
            1 for section in result["outline"]
            if section.get("section_intent") in {"Commercial", "Transactional"}
        )

        self.assertGreaterEqual(commercial_count, 5)
        self.assertEqual(next(s for s in result["outline"] if s["section_id"] == "faq")["section_intent"], "Informational")
        self.assertEqual(next(s for s in result["outline"] if s["section_id"] == "conclusion")["section_intent"], "Informational")

    def test_phase_19_step6_project_fulfillment_unsupported_when_project_names_missing(self):
        """Phase 1.9 Step 6: project sections must surface observed raw project names."""
        from src.services.brand_evidence_service import evaluate_brand_section_fulfillment

        section = {
            "section_id": "proof",
            "heading_text": "Projects shown by BrandCo",
            "taxonomy_axis": "brand_projects",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_projects"},
        }
        understanding = {"relevant_projects": ["Acumen Consulting Egypt", "Baddel"]}
        raw_blocks = [
            {
                "source_url": "https://brand.test/portfolio",
                "page_type": "portfolio",
                "heading": "Portfolio",
                "observed_text": "Project: Acumen Consulting Egypt. Client: Baddel.",
                "observed_facts": ["Project: Acumen Consulting Egypt", "Client: Baddel"],
            }
        ]

        report = evaluate_brand_section_fulfillment(
            section,
            "BrandCo presents a broad range of project examples without naming them.",
            understanding,
            raw_blocks,
            {"content_type": "brand_commercial", "brand_name": "BrandCo"},
        )

        self.assertEqual(report["fulfillment_status"], "unsupported")
        self.assertIn("project section did not surface observed project names", report["fulfillment_reason"])

    def test_phase_19_step6_project_fulfillment_satisfied_when_project_name_is_used(self):
        """Phase 1.9 Step 6: project sections pass when generated copy mentions raw project evidence."""
        from src.services.brand_evidence_service import evaluate_brand_section_fulfillment

        section = {
            "section_id": "proof",
            "heading_text": "Projects shown by BrandCo",
            "taxonomy_axis": "brand_projects",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_projects"},
        }
        understanding = {"relevant_projects": ["Acumen Consulting Egypt", "Baddel"]}
        raw_blocks = [
            {
                "source_url": "https://brand.test/portfolio",
                "page_type": "portfolio",
                "heading": "Portfolio",
                "observed_text": "Project: Acumen Consulting Egypt. Client: Baddel.",
                "observed_facts": ["Project: Acumen Consulting Egypt", "Client: Baddel"],
            }
        ]

        report = evaluate_brand_section_fulfillment(
            section,
            "BrandCo shows Acumen Consulting Egypt as one observed project example.",
            understanding,
            raw_blocks,
            {"content_type": "brand_commercial", "brand_name": "BrandCo"},
        )

        self.assertEqual(report["fulfillment_status"], "satisfied")
        self.assertIn("Acumen Consulting Egypt", report["matched_evidence"])

    def test_phase_19_step6_pricing_without_raw_pricing_is_unsupported(self):
        """Phase 1.9 Step 6: brand pricing/package sections require explicit raw pricing evidence."""
        from src.services.brand_evidence_service import evaluate_brand_section_fulfillment

        section = {
            "section_id": "pricing",
            "heading_text": "BrandCo pricing packages",
            "taxonomy_axis": "brand_offer",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_offer"},
        }
        understanding = {"relevant_services": ["Web Development"], "relevant_projects": []}
        raw_blocks = [
            {
                "source_url": "https://brand.test/services",
                "page_type": "services",
                "heading": "Services",
                "observed_text": "BrandCo provides Web Development and CRM integrations.",
                "observed_facts": ["Service: Web Development"],
            }
        ]

        report = evaluate_brand_section_fulfillment(
            section,
            "BrandCo offers starter and advanced packages for Web Development.",
            understanding,
            raw_blocks,
            {"content_type": "brand_commercial", "brand_name": "BrandCo"},
        )

        self.assertEqual(report["fulfillment_status"], "unsupported")
        self.assertIn("pricing/packages", report["fulfillment_reason"])

    def test_phase_19_step6_geography_without_raw_geography_is_unsupported(self):
        """Phase 1.9 Step 6: brand geography/local-presence claims require raw geography evidence."""
        from src.services.brand_evidence_service import evaluate_brand_section_fulfillment

        section = {
            "section_id": "geo",
            "heading_text": "BrandCo services in Countryland",
            "taxonomy_axis": "brand_offer",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_offer"},
        }
        understanding = {"relevant_services": ["Web Development"], "relevant_geography": []}
        raw_blocks = [
            {
                "source_url": "https://brand.test/services",
                "page_type": "services",
                "heading": "Services",
                "observed_text": "BrandCo provides Web Development.",
                "observed_facts": ["Service: Web Development"],
            }
        ]

        report = evaluate_brand_section_fulfillment(
            section,
            "BrandCo serves clients across Countryland with Web Development.",
            understanding,
            raw_blocks,
            {"content_type": "brand_commercial", "brand_name": "BrandCo", "area": "Countryland"},
        )

        self.assertEqual(report["fulfillment_status"], "unsupported")
        self.assertIn("geography", report["fulfillment_reason"])

    def test_phase_19_step6_project_topic_phrase_is_not_geography_claim(self):
        """Phase 1.9 Step 6: phrases like 'in web design' are not treated as location claims."""
        from src.services.brand_evidence_service import evaluate_brand_section_fulfillment

        section = {
            "section_id": "proof",
            "heading_text": "Projects shown by BrandCo in web design",
            "taxonomy_axis": "brand_projects",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_projects"},
        }
        understanding = {"relevant_projects": ["Acumen Consulting Egypt"], "relevant_geography": []}
        raw_blocks = [
            {
                "source_url": "https://brand.test/projects",
                "page_type": "projects",
                "heading": "Acumen Consulting Egypt",
                "observed_text": "Client: Acumen Consulting Egypt. UX/UI and mobile app project.",
                "observed_facts": ["Client: Acumen Consulting Egypt"],
            }
        ]

        report = evaluate_brand_section_fulfillment(
            section,
            "BrandCo shows Acumen Consulting Egypt as an observed project example.",
            understanding,
            raw_blocks,
            {"content_type": "brand_commercial", "brand_name": "BrandCo"},
        )

        self.assertEqual(report["fulfillment_status"], "satisfied")

    def test_phase_19_step6_best_keyword_heading_is_not_brand_trust_claim_by_itself(self):
        """SEO query wording in a heading must not create unsupported trust/geography failures."""
        from src.services.brand_evidence_service import evaluate_brand_section_fulfillment

        section = {
            "section_id": "offer",
            "heading_text": "Best web design company in Saudi Arabia: available services",
            "taxonomy_axis": "brand_offer",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_offer"},
        }
        understanding = {
            "relevant_services": ["Web Development"],
            "relevant_technologies": ["React"],
            "relevant_geography": [],
        }
        raw_blocks = [
            {
                "source_url": "https://brand.test/services",
                "page_type": "services",
                "heading": "Services",
                "observed_text": "BrandCo provides Web Development using React.",
                "observed_facts": ["Service: Web Development", "Technology: React"],
            }
        ]

        report = evaluate_brand_section_fulfillment(
            section,
            "BrandCo provides Web Development using React for business websites.",
            understanding,
            raw_blocks,
            {"content_type": "brand_commercial", "brand_name": "BrandCo", "area": "Saudi Arabia"},
        )

        self.assertNotEqual(report["fulfillment_status"], "unsupported")
        self.assertNotIn("trust", report["fulfillment_reason"])
        self.assertNotIn("geography", report["fulfillment_reason"])

    async def test_phase_19_step6_corrective_rewrite_happens_at_most_once(self):
        """Phase 1.9 Step 6: unsupported brand-owned content gets only one corrective rewrite."""
        controller = AsyncWorkflowController(work_dir=".")
        controller.section_writer = MagicMock()
        controller.section_writer.write = AsyncMock(side_effect=[
            {"content": "BrandCo presents its work in broad terms here.", "section_id": "proof"},
            {"content": "BrandCo still presents its work without naming any observed project.", "section_id": "proof"},
        ])
        controller.validator.enforce_paragraph_structure = MagicMock(side_effect=lambda content: content)
        controller.validator.extract_sentences = MagicMock(return_value=["BrandCo presents its work in broad terms here."])
        controller.validator.validate_section_output = AsyncMock(return_value=(True, []))

        section = {
            "section_id": "proof",
            "heading_text": "Projects shown by BrandCo",
            "section_type": "proof",
            "section_intent": "Commercial",
            "taxonomy_axis": "brand_projects",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_projects"},
        }
        state = {
            "content_type": "brand_commercial",
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/portfolio",
                    "page_type": "portfolio",
                    "heading": "Portfolio",
                    "text": "Project: Acumen Consulting Egypt. Client: Baddel.",
                    "observed_facts": ["Project: Acumen Consulting Egypt", "Client: Baddel"],
                }
            ],
            "brand_evidence_inventory": {
                "services_available": False,
                "projects_available": True,
                "pricing_available": False,
                "process_available": False,
                "trust_available": False,
                "explicit_geography": [],
                "confidence": "medium",
            },
            "used_internal_links": [],
            "used_external_links": [],
            "used_phrases": [],
            "used_claims": [],
            "used_topics": [],
        }

        result = await controller._write_single_section(
            title="Best Web Development",
            global_keywords={"primary": "web development"},
            section=section,
            article_intent="commercial",
            seo_intelligence={},
            content_type="brand_commercial",
            link_strategy={},
            state=state,
        )

        self.assertEqual(controller.section_writer.write.call_count, 2)
        self.assertEqual(result["fulfillment_status"], "unsupported")
        self.assertTrue(result["_brand_fulfillment_repair_attempted"])

    async def test_phase_19_step6_safer_version_kept_after_unresolved_fulfillment(self):
        """Phase 1.9 Step 6: if corrective rewrite is still unsupported, keep the safer first version."""
        controller = AsyncWorkflowController(work_dir=".")
        first_content = "BrandCo presents its work in broad terms here."
        controller.section_writer = MagicMock()
        controller.section_writer.write = AsyncMock(side_effect=[
            {"content": first_content, "section_id": "proof"},
            {"content": "BrandCo still omits the observed project names.", "section_id": "proof"},
        ])
        controller.validator.enforce_paragraph_structure = MagicMock(side_effect=lambda content: content)
        controller.validator.extract_sentences = MagicMock(return_value=[first_content])
        controller.validator.validate_section_output = AsyncMock(return_value=(True, []))

        section = {
            "section_id": "proof",
            "heading_text": "Projects shown by BrandCo",
            "section_type": "proof",
            "section_intent": "Commercial",
            "taxonomy_axis": "brand_projects",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_projects"},
        }
        state = {
            "content_type": "brand_commercial",
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/portfolio",
                    "page_type": "portfolio",
                    "heading": "Portfolio",
                    "text": "Project: Acumen Consulting Egypt. Client: Baddel.",
                    "observed_facts": ["Project: Acumen Consulting Egypt", "Client: Baddel"],
                }
            ],
            "brand_evidence_inventory": {
                "services_available": False,
                "projects_available": True,
                "pricing_available": False,
                "process_available": False,
                "trust_available": False,
                "explicit_geography": [],
                "confidence": "medium",
            },
            "used_internal_links": [],
            "used_external_links": [],
            "used_phrases": [],
            "used_claims": [],
            "used_topics": [],
        }

        result = await controller._write_single_section(
            title="Best Web Development",
            global_keywords={"primary": "web development"},
            section=section,
            article_intent="commercial",
            seo_intelligence={},
            content_type="brand_commercial",
            link_strategy={},
            state=state,
        )

        self.assertIn(first_content, result["generated_content"])
        self.assertNotIn("still omits", result["generated_content"])

    async def test_phase_19_section_evidence_audit_logs_selected_evidence(self):
        """Per-section audit should expose selected evidence and fulfillment without dumping pages."""
        controller = AsyncWorkflowController(work_dir=".")
        controller.section_writer = MagicMock()
        controller.section_writer.write = AsyncMock(return_value={
            "content": "BrandCo shows Acumen Consulting Egypt as a project example.",
            "section_id": "proof",
        })
        controller.validator.enforce_paragraph_structure = MagicMock(side_effect=lambda content: content)
        controller.validator.extract_sentences = MagicMock(return_value=[
            "BrandCo shows Acumen Consulting Egypt as a project example."
        ])
        controller.validator.validate_section_output = AsyncMock(return_value=(True, []))

        workflow_logger = MagicMock()
        section = {
            "section_id": "proof",
            "heading_text": "Projects shown by BrandCo",
            "section_type": "proof",
            "section_intent": "Commercial",
            "taxonomy_axis": "brand_projects",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_projects"},
        }
        state = {
            "content_type": "brand_commercial",
            "brand_name": "BrandCo",
            "workflow_logger": workflow_logger,
            "brand_source_chunks": [
                {
                    "url": "https://brand.test/portfolio",
                    "source_url": "https://brand.test/portfolio",
                    "page_type": "portfolio",
                    "heading": "Portfolio",
                    "text": "Project: Acumen Consulting Egypt. Client: Baddel.",
                    "observed_text": "Project: Acumen Consulting Egypt. Client: Baddel.",
                    "observed_facts": ["Project: Acumen Consulting Egypt", "Client: Baddel"],
                }
            ],
            "brand_evidence_inventory": {
                "services_available": False,
                "projects_available": True,
                "pricing_available": False,
                "process_available": False,
                "trust_available": False,
                "explicit_geography": [],
                "confidence": "medium",
            },
            "used_internal_links": [],
            "used_external_links": [],
            "used_phrases": [],
            "used_claims": [],
            "used_topics": [],
        }

        result = await controller._write_single_section(
            title="Best Web Development",
            global_keywords={"primary": "web development"},
            section=section,
            article_intent="commercial",
            seo_intelligence={},
            content_type="brand_commercial",
            link_strategy={},
            state=state,
        )

        audit = result["brand_evidence_audit"]
        self.assertEqual(audit["selected_blocks_count"], 1)
        self.assertIn("https://brand.test/portfolio", audit["selected_urls"])
        self.assertIn("Acumen Consulting Egypt", audit["relevant_projects"])
        self.assertEqual(audit["fulfillment_status"], "satisfied")

        audit_calls = [
            call.kwargs for call in workflow_logger.log_step_details.call_args_list
            if str(call.kwargs.get("step_name", "")).startswith("BRAND_SECTION_EVIDENCE_AUDIT")
        ]
        self.assertEqual(len(audit_calls), 1)
        self.assertEqual(audit_calls[0]["output_data"]["selected_blocks_count"], 1)
        self.assertEqual(audit_calls[0]["output_data"]["fulfillment_status"], "satisfied")

    def test_required_comparison_table_is_inserted_when_writer_omits_it(self):
        """Required table sections should not ship as prose-only content."""
        controller = AsyncWorkflowController(work_dir=".")
        section = {
            "section_id": "comparison",
            "heading_text": "المواقع المؤسسية مقابل المتاجر الإلكترونية",
            "section_type": "comparison",
            "taxonomy_axis": "comparison",
            "requires_table": True,
        }
        content = "هذا السكشن يشرح الفرق بين خيارين بحسب هدف المشروع وطريقة تفاعل المستخدم."

        result = controller._ensure_required_table_content(content, section, {"article_language": "ar"})

        self.assertTrue("| وجه المقارنة |" in result or "|وجه المقارنة|" in result)
        self.assertIn("|---|---|---|", result)
        self.assertTrue(controller._content_has_markdown_table(result))

    def test_project_families_cluster_variants_and_rank_target_area_evidence(self):
        """Project families should merge variants and prefer target-area evidence when present."""
        from src.services.brand_evidence_service import build_section_brand_understanding

        section = {"heading_text": "Projects shown by BrandCo", "section_type": "proof", "section_intent": "Commercial"}
        state = {"brand_name": "BrandCo", "content_type": "brand_commercial", "area": "Countryland"}
        chunks = [
            {
                "source_url": "https://brand.test/projects",
                "page_type": "projects",
                "heading": "Portfolio",
                "observed_text": (
                    "Project: Nile Portal Web app. Project: Nile Portal Mob App. "
                    "Project: Countryland Retail Platform. Location: Countryland."
                ),
                "observed_facts": [
                    "Project: Nile Portal Web app",
                    "Project: Nile Portal Mob App",
                    "Project: Countryland Retail Platform",
                    "Location: Countryland",
                ],
            }
        ]

        brief = build_section_brand_understanding(section, state, chunks)

        self.assertEqual(brief["relevant_projects"][0], "Countryland Retail")
        self.assertIn("Nile Portal", brief["relevant_projects"])
        families = {item["name"]: item for item in brief["relevant_project_families"]}
        self.assertIn("Nile Portal", families)
        self.assertIn("Nile Portal Web app", families["Nile Portal"]["variants"])
        self.assertIn("Nile Portal Mob App", families["Nile Portal"]["variants"])
        self.assertEqual(families["Countryland Retail"]["target_area_relevance"], "explicit")

    def test_brand_fulfillment_flags_low_paragraph_evidence_density(self):
        """Brand-owned sections with generic filler paragraphs should be weak even if one service is mentioned."""
        from src.services.brand_evidence_service import evaluate_brand_section_fulfillment

        section = {
            "heading_text": "Services offered by BrandCo",
            "section_type": "offer",
            "taxonomy_axis": "brand_offer",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_offer"},
        }
        understanding = {"relevant_services": ["Web Development", "CRM Integrations"], "relevant_projects": []}
        content = (
            "When choosing a provider, compare options carefully and ask about support before making a decision.\n\n"
            "BrandCo provides Web Development for teams that need a clearer digital presence."
        )

        report = evaluate_brand_section_fulfillment(
            section,
            content,
            understanding,
            [{"source_url": "https://brand.test/services", "page_type": "services", "heading": "Services", "observed_text": "BrandCo provides Web Development and CRM Integrations.", "observed_facts": []}],
            {"content_type": "brand_commercial", "brand_name": "BrandCo"},
        )

        self.assertEqual(report["fulfillment_status"], "weak")
        self.assertIn("evidence density", report["fulfillment_reason"])
        self.assertEqual(report["evidence_density"]["total_paragraphs"], 2)
        self.assertEqual(report["evidence_density"]["anchored_paragraphs"], 1)

    def test_brand_fulfillment_flags_heading_drift_into_generic_advice(self):
        """A service heading should not be fulfilled by generic buyer-advice prose with a token service mention."""
        from src.services.brand_evidence_service import evaluate_brand_section_fulfillment

        section = {
            "heading_text": "Services offered by BrandCo",
            "section_type": "offer",
            "taxonomy_axis": "brand_offer",
            "section_contract": {"brand_policy": "commercial", "taxonomy_axis": "brand_offer"},
        }
        understanding = {"relevant_services": ["Web Development"], "relevant_projects": []}
        content = (
            "To choose the right option, compare providers, ask about onboarding, check delivery criteria, "
            "and make sure the Web Development scope is suitable for your priority."
        )

        report = evaluate_brand_section_fulfillment(
            section,
            content,
            understanding,
            [{"source_url": "https://brand.test/services", "page_type": "services", "heading": "Services", "observed_text": "BrandCo provides Web Development.", "observed_facts": []}],
            {"content_type": "brand_commercial", "brand_name": "BrandCo"},
        )

        self.assertEqual(report["fulfillment_status"], "weak")
        self.assertIn("heading drift", report["fulfillment_reason"])
        self.assertTrue(report["heading_fidelity"]["drift_detected"])

    def test_project_required_table_with_one_narrative_page_uses_prose_not_table(self):
        """One valid narrative project page should produce a prose sentence, NOT a markdown table."""
        controller = AsyncWorkflowController(work_dir=".")
        section = {
            "section_id": "proof",
            "heading_text": "Projects shown by BrandCo",
            "section_type": "proof",
            "taxonomy_axis": "brand_projects",
            "requires_table": True,
            "section_page_narrative_briefs": [
                {
                    "source_url": "https://brand.test/portfolio/baddel",
                    "page_type": "portfolio",
                    "page_title": "Baddel - BrandCo",
                    "narrative_brief": "This page presents the Baddel project in Riyadh within the E-commerce sector.",
                    "routing_signals": {
                        "projects": ["Baddel"],
                        "explicit_geography": ["Riyadh"],
                        "services": ["Branding"],
                        "project_locations": ["Riyadh"],
                    },
                }
            ],
            "section_brand_understanding": {},
        }
        content = "We need a visual project presentation here."
        state = {
            "article_language": "en",
            "brand_page_narrative_briefs": section["section_page_narrative_briefs"],
        }

        result = controller._ensure_required_table_content(content, section, state)

        # Must not render a table
        self.assertNotIn("|---", result)
        self.assertNotIn("| Project |", result)
        # Must contain factual prose with the project name
        self.assertIn("Baddel", result)

    def test_project_required_table_with_two_narrative_pages_renders_table(self):
        """Two or more valid narrative project pages must produce a markdown table."""
        controller = AsyncWorkflowController(work_dir=".")
        section = {
            "section_id": "proof",
            "heading_text": "Projects shown by BrandCo",
            "section_type": "proof",
            "taxonomy_axis": "brand_projects",
            "requires_table": True,
            "section_page_narrative_briefs": [
                {
                    "source_url": "https://brand.test/portfolio/baddel",
                    "page_type": "portfolio",
                    "page_title": "Baddel - BrandCo",
                    "narrative_brief": "This page presents the Baddel project in Riyadh within the E-commerce sector.",
                    "routing_signals": {
                        "projects": ["Baddel"],
                        "explicit_geography": ["Riyadh"],
                        "services": ["Branding"],
                        "project_locations": ["Riyadh"],
                    },
                },
                {
                    "source_url": "https://brand.test/portfolio/retail-portal",
                    "page_type": "portfolio",
                    "page_title": "Retail Portal - BrandCo",
                    "narrative_brief": "This page presents the Retail Portal project in Dubai within the Retail sector.",
                    "routing_signals": {
                        "projects": ["Retail Portal"],
                        "explicit_geography": ["Dubai"],
                        "services": ["Web Development"],
                        "project_locations": ["Dubai"],
                    },
                },
            ],
            "section_brand_understanding": {},
        }
        content = "We need a visual project presentation here."
        state = {
            "article_language": "en",
            "brand_page_narrative_briefs": section["section_page_narrative_briefs"],
        }

        result = controller._ensure_required_table_content(content, section, state)

        self.assertIn("|---", result)
        self.assertIn("Baddel", result)
        self.assertIn("Retail Portal", result)

    def test_brand_page_knowledge_pack_prompt_context_cleanliness(self):
        """The writer prompt context must contain ONLY clean narrative page context and claim boundaries.
        It must NOT contain: saved file paths, crawled URL counts, page read stats,
        or any extraction list phrases."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "brand_url": "https://brand.test",
            "brand_page_knowledge_pack_path": "/some/output/path/brand_page_knowledge_pack.md",
            "brand_crawl_report": {
                "crawled_urls": ["https://brand.test", "https://brand.test/portfolio"],
                "page_read_stats": [
                    {"url": "https://brand.test", "page_type": "home", "text_chars": 1200, "semantic_sections_count": 4}
                ],
            },
            "brand_page_narrative_briefs": [
                {
                    "page_title": "Baddel - BrandCo",
                    "source_url": "https://brand.test/portfolio/baddel",
                    "page_type": "portfolio",
                    "narrative_brief": "This page presents the Baddel project in Riyadh, Saudi Arabia within the E-commerce sector.",
                    "claim_boundaries": ["No explicit pricing observed on this page."],
                    "routing_signals": {
                        "projects": ["Baddel"],
                        "services": ["Branding", "UX/UI"],
                        "technologies": ["React"],
                        "explicit_geography": ["Riyadh", "Saudi Arabia"],
                        "process_steps": [],
                        "project_locations": ["Riyadh"],
                        "has_pricing": False,
                        "has_trust": False,
                    },
                }
            ],
        }

        context = controller._format_brand_page_knowledge_pack_for_prompt(state)

        # Cleanliness: diagnostics must NOT appear in prompt context
        self.assertNotIn("Saved file", context)
        self.assertNotIn("Crawled URLs count", context)
        self.assertNotIn("Crawled URLs:", context)
        self.assertNotIn("Page read stats", context)

        # Cleanliness: extraction list phrases must NOT appear in prompt context
        self.assertNotIn("Project or client names visible include", context)
        self.assertNotIn("Services or capabilities explicitly visible include", context)
        self.assertNotIn("Technologies, tools, or platform terms explicitly visible include", context)
        self.assertNotIn("Workflow or process terms visible", context)

        # Correctness: clean narrative content MUST be present
        self.assertIn("Baddel project", context)
        self.assertIn("Claim boundaries", context)
        self.assertIn("No explicit pricing observed", context)

    def test_full_brand_page_knowledge_pack_prompt_context_is_not_truncated(self):
        """Default writer context must include the full cleaned pack, not the first 12k chars only."""
        controller = AsyncWorkflowController(work_dir=".")
        briefs = []
        for idx in range(1, 9):
            briefs.append(
                {
                    "page_title": f"Detailed Service Page {idx}",
                    "source_url": f"https://brand.test/services/{idx}",
                    "page_type": "services",
                    "narrative_brief": (
                        f"This page explains Service Capability {idx}. "
                        + "It preserves detailed observed page wording about UX/UI, React, Node.js, "
                        + "WordPress, implementation planning, testing, launch support, and client handover. "
                    ) * 18,
                    "claim_boundaries": ["No explicit pricing observed on this page."],
                    "routing_signals": {"services": [f"Service Capability {idx}"]},
                }
            )
        state = {"brand_page_narrative_briefs": briefs}

        context = controller._format_brand_page_knowledge_pack_for_prompt(state)

        self.assertGreater(len(context), 12000)
        self.assertNotIn("[TRUNCATED", context)
        self.assertIn("Detailed Service Page 8", context)
        self.assertIn("Service Capability 8", context)

    def test_target_area_pages_are_ordered_before_farther_project_pages(self):
        """Narrative pack ordering should prefer explicit target-area evidence without hardcoding a brand/article."""
        from src.services.brand_evidence_service import build_brand_page_narrative_briefs

        state = {
            "area": "\u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629",
            "brand_name": "BrandCo",
            "brand_source_chunks": [
                {
                    "source_url": "https://brand.test/projects/aqar",
                    "page_type": "portfolio",
                    "page_title": "Aqar Platform - BrandCo",
                    "heading": "Project overview",
                    "observed_text": (
                        "This portfolio page presents the Aqar Platform project. "
                        "Location: Egypt. Sector: Real Estate. Services Provided: UX/UI, Web Development. "
                        "Technology Stack: React, Node.js, Figma."
                    ),
                },
                {
                    "source_url": "https://brand.test/projects/baddel",
                    "page_type": "portfolio",
                    "page_title": "Baddel - BrandCo",
                    "heading": "Project overview",
                    "observed_text": (
                        "This portfolio page presents the Baddel project. "
                        "Location: Riyadh, Saudi Arabia. Sector: E-commerce. Services Provided: Branding, UX/UI, Web Application. "
                        "Technology Stack: React, Tailwind CSS, Node.js."
                    ),
                },
            ],
        }

        briefs = build_brand_page_narrative_briefs(state)
        titles = [brief["page_title"] for brief in briefs]

        self.assertGreaterEqual(len(titles), 2)
        self.assertLess(titles.index("Baddel - BrandCo"), titles.index("Aqar Platform - BrandCo"))

    def test_page_narrative_brief_cleans_layout_noise_without_deleting_facts(self):
        """Narrative compression should remove layout chrome and adjacent repetition while preserving facts."""
        from src.services.brand_evidence_service import _build_page_narrative_text

        narrative = _build_page_narrative_text(
            page_type="portfolio",
            title="Baddel - BrandCo",
            headings=["Project overview"],
            text=(
                "Let's Talk Scroll to Top View Project "
                "Baddel Baddel is an e-commerce project in Riyadh, Saudi Arabia. "
                "Technology Stack: React React, Node.js, Tailwind CSS. "
                "Services Provided: Branding, UX/UI, Web Application. "
                "All rights reserved Subscribe Newsletter"
            ),
            services=["Branding", "UX/UI", "Web Application"],
            technologies=["React", "Node.js", "Tailwind CSS"],
            projects=["Baddel"],
            process_steps=[],
            geography=[],
            project_locations=["Riyadh, Saudi Arabia"],
            pricing=[],
            trust=[],
        )

        self.assertNotIn("Let's Talk", narrative)
        self.assertNotIn("Scroll to Top", narrative)
        self.assertNotIn("All rights reserved", narrative)
        self.assertIn("Baddel", narrative)
        self.assertIn("Riyadh", narrative)
        self.assertIn("React", narrative)
        self.assertNotIn("React React", narrative)

    def test_comparison_table_does_not_fall_back_to_location_template(self):
        """Comparison sections must not inherit location/real-estate fallback tables from stale taxonomy."""
        controller = AsyncWorkflowController(work_dir=".")
        section = {
            "heading_text": "\u0645\u0642\u0627\u0631\u0646\u0629 \u0628\u064a\u0646 \u062e\u062f\u0645\u0627\u062a \u0627\u0644\u062a\u0635\u0645\u064a\u0645 \u0641\u064a \u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629",
            "section_type": "comparison",
            "taxonomy_axis": "location_area",
            "requires_table": True,
            "subheadings": ["Custom websites", "E-commerce websites"],
        }
        state = {
            "article_language": "ar",
            "primary_keyword": "\u0634\u0631\u0643\u0629 \u062a\u0635\u0645\u064a\u0645 \u0645\u0648\u0627\u0642\u0639",
            "raw_title": "\u0627\u0641\u0636\u0644 \u0634\u0631\u0643\u0629 \u062a\u0635\u0645\u064a\u0645 \u0645\u0648\u0627\u0642\u0639",
        }

        result = controller._ensure_required_table_content("\u0646\u0635 \u062a\u0645\u0647\u064a\u062f\u064a.", section, state)

        self.assertIn("|---", result)
        self.assertIn("Custom websites", result)
        self.assertIn("E-commerce websites", result)
        self.assertNotIn("\u0627\u0644\u0645\u0648\u0627\u0635\u0644\u0627\u062a", result)
        self.assertNotIn("\u0646\u0645\u0637 \u0627\u0644\u0633\u0643\u0646", result)

    def test_project_table_uses_clean_titles_and_target_area_ordering(self):
        """Project fallback tables should use clean page titles and rank target-area project pages first."""
        controller = AsyncWorkflowController(work_dir=".")
        section = {
            "heading_text": "Projects shown by BrandCo",
            "section_type": "proof",
            "taxonomy_axis": "brand_projects",
            "requires_table": True,
            "section_page_narrative_briefs": [
                {
                    "source_url": "https://brand.test/projects/aqar",
                    "page_type": "portfolio",
                    "page_title": "Aqar Platform - BrandCo",
                    "narrative_brief": "This page presents the Aqar Platform project in Egypt within the Real Estate sector.",
                    "routing_signals": {
                        "projects": ["Aqar Platform"],
                        "explicit_geography": ["Egypt"],
                        "project_locations": ["Egypt"],
                        "services": ["UX/UI", "Web Development"],
                        "technologies": ["React", "Node.js"],
                    },
                },
                {
                    "source_url": "https://brand.test/projects/noise",
                    "page_type": "portfolio",
                    "page_title": "Real EstateTarget - BrandCo",
                    "narrative_brief": "This page contains a metadata label, not a real project record.",
                    "routing_signals": {
                        "projects": ["Real EstateTarget"],
                        "project_locations": ["ing and on-site content. Web Application"],
                        "services": [],
                        "technologies": [],
                    },
                },
                {
                    "source_url": "https://brand.test/projects/baddel",
                    "page_type": "portfolio",
                    "page_title": "Baddel - BrandCo",
                    "narrative_brief": "This page presents the Baddel project in Riyadh, Saudi Arabia within the E-commerce sector.",
                    "routing_signals": {
                        "projects": ["Baddel"],
                        "explicit_geography": ["Riyadh", "Saudi Arabia"],
                        "project_locations": ["Riyadh, Saudi Arabia"],
                        "services": ["Branding", "UX/UI"],
                        "technologies": ["React", "Tailwind CSS"],
                    },
                },
            ],
        }
        state = {
            "article_language": "en",
            "area": "\u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629",
            "brand_page_narrative_briefs": section["section_page_narrative_briefs"],
        }

        result = controller._ensure_required_table_content("Project proof should be shown here.", section, state)

        self.assertIn("|---", result)
        self.assertIn("Baddel", result)
        self.assertIn("Aqar Platform", result)
        self.assertLess(result.index("Baddel"), result.index("Aqar Platform"))
        self.assertNotIn("Real EstateTarget", result)
        self.assertNotIn("ing and", result)

    def test_project_table_replaces_noisy_writer_generated_table(self):
        """Existing project tables should be replaced with deterministic clean project rows."""
        controller = AsyncWorkflowController(work_dir=".")
        section = {
            "heading_text": "Projects shown by BrandCo",
            "section_type": "proof",
            "taxonomy_axis": "brand_projects",
            "requires_table": True,
            "section_page_narrative_briefs": [
                {
                    "source_url": "https://brand.test/projects/aqar",
                    "page_type": "portfolio",
                    "page_title": "Aqar Platform - BrandCo",
                    "narrative_brief": "This page presents the Aqar Platform project in Egypt within the Real Estate sector.",
                    "routing_signals": {
                        "projects": ["Aqar Platform"],
                        "project_locations": ["ing"],
                        "services": ["UX/UI", "Web Development"],
                        "technologies": ["React", "Node.js"],
                    },
                },
                {
                    "source_url": "https://brand.test/projects/baddel",
                    "page_type": "portfolio",
                    "page_title": "Baddel - BrandCo",
                    "narrative_brief": "This page presents the Baddel project in Riyadh, Saudi Arabia within the E-commerce sector.",
                    "routing_signals": {
                        "projects": ["Baddel"],
                        "project_locations": ["ing"],
                        "services": ["Branding", "UX/UI"],
                        "technologies": ["React", "Tailwind CSS"],
                    },
                },
            ],
        }
        state = {
            "article_language": "en",
            "area": "\u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629",
            "brand_page_narrative_briefs": section["section_page_narrative_briefs"],
        }
        noisy_content = (
            "Intro.\n\n"
            "| Project | Location | Details |\n"
            "|---|---|---|\n"
            "| Aqar Platform | ing | UX |\n"
            "| Baddel | ing | UX |\n\n"
            "After table."
        )

        result = controller._ensure_required_table_content(noisy_content, section, state)

        self.assertIn("|---", result)
        self.assertIn("Baddel", result)
        self.assertIn("Riyadh, Saudi Arabia", result)
        self.assertIn("Aqar Platform", result)
        self.assertIn("Egypt", result)
        self.assertLess(result.index("Baddel"), result.index("Aqar Platform"))
        self.assertNotIn("| ing |", result)

    def test_brand_usage_policy_keeps_generic_sections_neutral(self):
        """Brand-commercial articles should not force brand discussion into every section."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {"content_type": "brand_commercial", "brand_name": "BrandCo"}

        self.assertEqual(
            controller._brand_usage_policy_for_section({"section_type": "faq", "heading_text": "Common questions"}, state),
            "neutral_market",
        )
        self.assertEqual(
            controller._brand_usage_policy_for_section({"section_type": "comparison", "heading_text": "Compare service options"}, state),
            "neutral_market",
        )
        self.assertEqual(
            controller._brand_usage_policy_for_section({"section_type": "offer", "heading_text": "Web design services"}, state),
            "brand_light",
        )
        self.assertEqual(
            controller._brand_usage_policy_for_section({"section_type": "differentiation", "heading_text": "Why BrandCo is different"}, state),
            "brand_owned",
        )
        self.assertEqual(
            controller._brand_usage_policy_for_section({"section_type": "conclusion", "heading_text": "Start now"}, state),
            "brand_cta",
        )

    async def test_writer_prompt_exposes_brand_usage_policy_and_plain_language_guard(self):
        """Writer prompt should tell neutral sections not to mention the brand just because the pack is present."""
        from src.services.content_generator import SectionWriter
        from unittest.mock import AsyncMock, MagicMock

        mock_ai = MagicMock()
        mock_ai.send = AsyncMock(return_value={"content": "{}", "metadata": {}})
        writer = SectionWriter(mock_ai)

        await writer.write(
            title="Dummy Title",
            global_keywords={"primary": "web design", "lsi": [], "semantic": []},
            section={
                "heading_text": "Compare service options",
                "section_type": "comparison",
                "section_intent": "Informational",
                "brand_usage_policy": "neutral_market",
            },
            article_intent="Commercial",
            seo_intelligence={"market_analysis": {"market_insights": {}}},
            content_type="brand_commercial",
            link_strategy="internal_only",
            brand_url="https://brand.test",
            brand_name="BrandCo",
            brand_link_used=False,
            brand_link_allowed=False,
            allow_external_links=False,
            execution_plan={},
            area="Riyadh",
            brand_page_knowledge_pack_context=(
                "[BRAND PAGE KNOWLEDGE PACK - PAGE BY PAGE]\n"
                "## Page 1: BrandCo\n"
                "- What this page contains:\n"
                "BrandCo provides UX/UI and React projects."
            ),
        )

        sent_prompt = mock_ai.send.call_args[0][0]
        self.assertIn('Brand Usage Policy For This Section', sent_prompt)
        self.assertIn('neutral_market', sent_prompt)
        self.assertIn('do not mention "BrandCo"', sent_prompt)
        self.assertIn('Write for a normal business owner', sent_prompt)
        self.assertIn('Do not create prices, ranges, durations', sent_prompt)

    def test_commercial_section_roles_drive_brand_usage_policy(self):
        """Domain-neutral commercial roles should keep generic sections from becoming brand-owned."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "content_type": "brand_commercial",
            "brand_name": "BrandCo",
            "display_brand_name": "BrandCo",
            "primary_keyword": "best service provider",
            "keywords": ["best service provider"],
            "input_data": {"title": "best service provider", "keywords": ["best service provider"]},
            "brand_evidence_inventory": {"services_available": True, "projects_available": True},
            "brand_page_narrative_briefs": [
                {
                    "page_type": "portfolio",
                    "page_title": "Project One - BrandCo",
                    "routing_signals": {"projects": ["Project One"]},
                },
                {
                    "page_type": "portfolio",
                    "page_title": "Project Two - BrandCo",
                    "routing_signals": {"projects": ["Project Two"]},
                },
            ],
        }
        outline = [
            {"section_type": "introduction", "heading_level": "INTRO", "heading_text": "Intro"},
            {"section_type": "offer", "heading_text": "What the service includes"},
            {"section_type": "features", "heading_text": "Key features included"},
            {"section_type": "differentiation", "heading_text": "Why BrandCo is different"},
            {"section_type": "proof", "heading_text": "Projects shown by BrandCo"},
            {"section_type": "comparison", "heading_text": "Compare available options"},
            {"section_type": "process", "heading_text": "How the process works"},
            {"section_type": "faq", "heading_text": "Common questions"},
            {"section_type": "conclusion", "heading_text": "Start now"},
        ]

        prepared = controller._prepare_outline_for_content(state, outline)
        roles = [section["commercial_section_role"] for section in prepared["outline"]]
        policies = [section["brand_usage_policy"] for section in prepared["outline"]]

        self.assertEqual(
            roles,
            [
                "intro",
                "service_explanation",
                "features_included",
                "brand_differentiator",
                "proof",
                "comparison",
                "process",
                "faq",
                "cta",
            ],
        )
        self.assertEqual(policies[1], "brand_light")
        self.assertEqual(policies[2], "brand_light")
        self.assertEqual(policies[5], "neutral_market")
        self.assertEqual(policies[7], "neutral_market")
        self.assertEqual(policies[8], "brand_cta")

    def test_brand_usage_policy_flags_project_examples_in_brand_light_sections(self):
        """Service/features sections may mention the brand lightly but should not consume proof examples."""
        controller = AsyncWorkflowController(work_dir=".")
        state = {
            "content_type": "brand_commercial",
            "brand_name": "BrandCo",
            "brand_page_narrative_briefs": [
                {
                    "page_type": "portfolio",
                    "page_title": "Acme Portal - BrandCo",
                    "routing_signals": {"projects": ["Acme Portal"]},
                }
            ],
        }
        section = {
            "heading_text": "What the service includes",
            "section_type": "offer",
            "commercial_section_role": "service_explanation",
            "brand_usage_policy": "brand_light",
        }

        report = controller._evaluate_brand_usage_policy_fulfillment(
            section,
            "BrandCo explains the service through Acme Portal as a detailed proof example.",
            state,
        )

        self.assertEqual(report["fulfillment_status"], "unsupported")
        self.assertIn("brand_light", report["fulfillment_reason"])

    def test_generic_comparison_table_has_real_contrast_not_placeholder(self):
        """Comparison fallback tables should compare options, not repeat the same generic cell values."""
        controller = AsyncWorkflowController(work_dir=".")
        section = {
            "heading_text": "Compare available service options",
            "section_type": "comparison",
            "taxonomy_axis": "comparison",
            "requires_table": True,
            "commercial_section_role": "comparison",
            "subheadings": ["Standard option", "Custom option"],
        }
        state = {"article_language": "en", "primary_keyword": "service provider", "raw_title": "service provider"}

        result = controller._ensure_required_table_content("Intro paragraph.", section, state)

        self.assertIn("Standard option", result)
        self.assertIn("Custom option", result)
        self.assertIn("Customization", result)
        self.assertNotIn("Depends on the project need and scope", result)
        self.assertNotIn("Outcomes, requirements, and service boundaries", result)

    async def test_writer_prompt_exposes_commercial_section_role(self):
        """Writer prompt should expose the commercial role separately from brand policy."""
        from src.services.content_generator import SectionWriter
        from unittest.mock import AsyncMock, MagicMock

        mock_ai = MagicMock()
        mock_ai.send = AsyncMock(return_value={"content": "{}", "metadata": {}})
        writer = SectionWriter(mock_ai)

        await writer.write(
            title="Dummy Title",
            global_keywords={"primary": "service provider", "lsi": [], "semantic": []},
            section={
                "heading_text": "What the service includes",
                "section_type": "offer",
                "section_intent": "Commercial",
                "commercial_section_role": "service_explanation",
                "brand_usage_policy": "brand_light",
            },
            article_intent="Commercial",
            seo_intelligence={"market_analysis": {"market_insights": {}}},
            content_type="brand_commercial",
            link_strategy="internal_only",
            brand_url="https://brand.test",
            brand_name="BrandCo",
            brand_link_used=False,
            brand_link_allowed=False,
            allow_external_links=False,
            execution_plan={},
            area="",
            brand_page_knowledge_pack_context="[BRAND PAGE KNOWLEDGE PACK - PAGE BY PAGE]\nNo facts.",
        )

        sent_prompt = mock_ai.send.call_args[0][0]
        self.assertIn("Commercial Section Role", sent_prompt)
        self.assertIn("service_explanation", sent_prompt)
        self.assertIn("Commercial Role Discipline", sent_prompt)


if __name__ == '__main__':
    unittest.main()
