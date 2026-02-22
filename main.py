import subprocess
import sys
from src.utils.logger import get_logger

logger = get_logger(__name__)

def run_step(step_name: str, module_path: str) -> bool:
    """
    Executes a Python module as a separate thread.
    Returns True if successful, False if failed.
    """
    logger.info(f"--- Starting Phase: {step_name} ---")
    try:
        result = subprocess.run(
            [sys.executable, "-m", module_path],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(f"Phase '{step_name}' successfully completed.")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Fatal error in the phase '{step_name}'. Code: {e.returncode}")
        # If the subprocess fails, print the error
        logger.error(f"Error details:\n{e.stderr}")
        return False

def main():
    logger.info("=== INITIALIZING PIPELINE E2E MARKET SENTIMENT ===")

    # Bronze Layer (Ingestion)
    if not run_step("API Extraction Market", "src.ingestion.api_market"): 
        return
    if not run_step("Yahoo News Scraping", "src.ingestion.news_scraper"): 
        return

    # Silver Layer (Processing, NLP and Cleaning)
    if not run_step("Spark Silver Processing", "src.processing.silver_layer"): 
        return

    # Gold Layer (Business Modeling and SQL Cross-Relationship)
    if not run_step("Gold Analytical Junction", "src.processing.gold_layer"): 
        return

    logger.info("=== PIPELINE E2E SUCCESFULLY ENDED ===")

if __name__ == "__main__":
    main()