import requests
from bs4 import BeautifulSoup
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict

# Logging configuration
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YahooFinanceScraper:
    """
    Scraper for extracting financial news headlines.
    """
    TARGET_URL = "https://finance.yahoo.com/topic/stock-market-news/"

    def __init__(self, base_path: str = "data/bronze/news"):
        self.base_path = Path(base_path)
        # Check if the directory exists
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Header
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

    def fetch_html(self) -> str:
        """Download the complete HTML tree of the page."""
        try:
            logger.info(f"Conecting to {self.TARGET_URL}...")
            response = requests.get(self.TARGET_URL, headers=self.headers, timeout=15)
            response.raise_for_status()
            logger.info("HTML downloaded succesfully.")
            return response.text
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP communication failure: {e}")
            return ""
        
    def parse_headlines(self, html_content: str) -> List[Dict[str, str]]:
        """Analyze the HTML and extract the headlines."""
        if not html_content:
            return []

        # Convert plain text into a navigable object
        soup = BeautifulSoup(html_content, 'html.parser')
        news_data = []

        # Look for headlines
        h3_nodes = soup.find_all('h3')
        
        for node in h3_nodes:
            title = node.get_text(strip=True)
            
            # A real headline usually has more than 15 characters.
            if len(title) > 15:
                news_data.append({
                    "source": "yahoo_finance",
                    "title": title,
                    "scraped_at": datetime.now().isoformat()
                })
        
        logger.info(f"Extraction completed: {len(news_data)} headlines found.")
        return news_data
    
    def save_raw_data(self, data: List[Dict[str, str]]):
        """Save the data in the Bronze Layer (Local Data Lake)."""
        if not data:
            logger.warning("Empty list. No data to save..")
            return

        today = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%H%M%S")
        
        target_dir = self.base_path / today
        target_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = target_dir / f"headlines_{timestamp}.json"

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            logger.info(f"News saved in: {file_path}")
        except IOError as e:
            logger.error(f"I/O error writing to disk: {e}")

if __name__ == "__main__":
    
    scraper = YahooFinanceScraper()
    raw_html = scraper.fetch_html()
    headlines = scraper.parse_headlines(raw_html)
    scraper.save_raw_data(headlines)