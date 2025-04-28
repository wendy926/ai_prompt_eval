import json
import os
import logging
from dotenv import load_dotenv
from src.models.gemini_model import GeminiDialogueAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    try:
        logging.info("Loading environment variables...")
        load_dotenv()
        api_key = os.getenv("GOOGLE_API_KEY")
        
        if not api_key:
            raise ValueError("Missing GOOGLE_API_KEY in environment variables")
        
        logging.info("Initializing GeminiDialogueAnalyzer...")
        analyzer = GeminiDialogueAnalyzer(api_key=api_key)
        
        logging.info("Starting dialogue analysis...")
        result = analyzer.analyze_dialogue(submitter="AI prompt")
        
        logging.info("Analysis completed. Formatting output...")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        logging.info("Program finished successfully.")
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    logging.info("Starting the program...")
    main()
