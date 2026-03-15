import os
import sys

# Add project root to sys.path to resolve module imports 
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import uvicorn
import logging
from src.app.api import app

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    print("Starting SEO Writing AI API Server on http://localhost:8000")
    print("Access the API Documentation at http://localhost:8000/docs")
    uvicorn.run("src.app.api:app", host="0.0.0.0", port=8000, reload=True)

if __name__ == "__main__":
    main()
