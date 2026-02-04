import os
import logging
import json
from services.workflow_controller import WorkflowController
from schemas.input_validator import ArticleInput

# Configure logging to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    print("==========================================")
    print("      SEO ARTICLE GENERATOR AI            ")
    print("==========================================")
    
    # Check API Key
    if not os.getenv("OPENROUTER_API_KEY"):
        print("\n[WARNING] OPENROUTER_API_KEY environment variable is not set.")
        print("Please set it before running, or the LLM calls will fail.\n")
        # In a real app we might exit or ask for input, but for now we proceed to let the user see the flow if they have a .env file loaded otherwise
    
    # Interactive Input
    print("\nEnter Article Details (or press Enter for defaults):")
    title = input("Target Keyword / Title: ").strip()
    
    if not title:
        print("No input provided. Using TEST mode.")
        user_input = {
            "title": "How to Start a Vegetable Garden for Beginners",
            "keywords": ["vegetable gardening", "home garden", "planting tips"],
            "urls": []
        }
    else:
        keywords_str = input("Keywords (comma separated): ").strip()
        keywords = [k.strip() for k in keywords_str.split(",")] if keywords_str else [title]
        
        user_input = {
            "title": title,
            "keywords": keywords,
            "urls": []
        }
        
    print(f"\nProcessing: {user_input['title']}...")
    print("------------------------------------------")

    try:
        # Initialize Controller
        controller = WorkflowController()
        
        # Run Workflow
        result_state = controller.run_workflow(user_input)
        
        # Output Results
        final_file = os.path.join("output", "article_final.md")
        report = result_state.get("validation_report", {})
        
        print("\n==========================================")
        print("           GENERATION COMPLETE            ")
        print("==========================================")
        print(f"Article saved to: {os.path.abspath(final_file)}")
        print(f"Images generated: {len(result_state.get('images', []))}")
        
        if report:
             print(f"SEO Check: {'PASSED' if report.get('passed') else 'FAILED'}")
             if report.get('errors'):
                 print("Errors:", report['errors'])
             if report.get('warnings'):
                 print("Warnings:", report['warnings'])
        
    except Exception as e:
        logger.exception("An error occurred during execution:")
        print(f"\n[ERROR] {e}")

if __name__ == "__main__":
    main()
