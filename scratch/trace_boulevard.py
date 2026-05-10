import asyncio
import logging
import json
import os
import sys

# Setup logging to stdout
logging.basicConfig(level=logging.DEBUG, format='%(message)s')
logger = logging.getLogger()

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.services.serp_topic_miner import SERPTopicMiner
from src.services.research_service import ResearchService

async def main():
    miner = SERPTopicMiner()
    research = ResearchService(ai_client=None, work_dir=os.getcwd())
    
    # Mock SERP data for "Boulevard City Riyadh"
    serp_data = {
        "top_results": [
            {
                "title": "Boulevard City Riyadh: Tickets, Hours, and Guide 2024",
                "snippet": "Discover the best activities at Boulevard City Riyadh. Book your tickets now and enjoy the unique experience.",
                "headings": {
                    "h2": [
                        "Everything You Need to Know About Boulevard City",
                        "Main Attractions and Activities",
                        "Boulevard City Riyadh Tickets and Pricing",
                        "Opening Hours and Best Time to Visit",
                        "How to Get to Boulevard City - complete guide"
                    ],
                    "h3": [
                        "Garden area details",
                        "Music venue highlights",
                        "Parking at Boulevard City"
                    ]
                }
            },
            {
                "title": "بوليفارد سيتي الرياض: دليل شامل للتذاكر والفعاليات",
                "snippet": "احجز تذاكر بوليفارد سيتي الرياض واستمتع بأجمل الفعاليات والأنشطة الترفيهية.",
                "headings": {
                    "h2": [
                        "ما هو بوليفارد سيتي الرياض؟ التجربة الفريدة",
                        "فعاليات بوليفارد سيتي 2024",
                        "أسعار تذاكر بوليفارد سيتي الرياض",
                        "مواعيد بوليفارد سيتي الرياض",
                        "موقع بوليفارد سيتي وكيفية الوصول"
                    ]
                }
            }
        ],
        "lsi_keywords": ["Boulevard City events", "Riyadh Season Boulevard", "حجز تذاكر بوليفارد", "موسم الرياض"],
        "paa_questions": ["How much are Boulevard City tickets?", "When does Boulevard City open?"],
        "related_searches": ["Boulevard City restaurants", "Boulevard City parking"]
    }
    
    pk = "Boulevard City Riyadh"
    brand = "Tikevent"
    
    mining_results = miner.mine_topics(serp_data, pk, brand)
    
    # Mocking state and seo_intelligence
    seo_intelligence = {
        "serp_raw": serp_data,
        "market_analysis": {
            "intent_analysis": {"confirmed_intent": "informational", "dominant_page_type": "guide"},
            "structural_intelligence": {"dominant_heading_pattern": "Guide structure"},
            "market_insights": {
                "writing_guide": "Write a clear guide.", 
                "topic_observations": {"core_recurring_topics": [], "secondary_mentions": []},
                "avoid_sections": ["Gallery"]
            }
        }
    }
    state = {"primary_keyword": pk, "brand_name": brand, "seo_intelligence": seo_intelligence}
    
    brief = research.build_serp_outline_brief(state)
    
    output = {
        "mining_results": mining_results,
        "brief": brief
    }
    
    with open("scratch/trace_output.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("\n--- TRACE COMPLETE: Output written to scratch/trace_output.json ---")

if __name__ == "__main__":
    asyncio.run(main())
