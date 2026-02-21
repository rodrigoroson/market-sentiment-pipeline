import requests
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

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

class CoinGeckoIngestor:
    """
    Responsible for ingesting market data from CoinGecko.
    Pattern: Extract (API) -> Load
    """
    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, base_path: str = "data/bronze/market"):
        """
        Args:
            base_path: Path where raw data is saved.
        """
        self.base_path = Path(base_path)
        # Check the directory exists
        self.base_path.mkdir(parents=True, exist_ok=True)

    def fetch_current_price(self, coin_ids: List[str], vs_currencies: str = "usd") -> Dict[str, Any]:
        """
        Consult the API for current prices.

        Args:
            param coin_ids: List of coin IDs (e.g., ['bitcoin', 'ethereum'])
            
        Return: Dictionary with the raw JSON response.
        """
        endpoint = f"{self.BASE_URL}/simple/price"
        params = {
            "ids": ",".join(coin_ids),
            "vs_currencies": vs_currencies,
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true",
            "include_last_updated_at": "true"
        }

        try:
            logger.info(f"Consulting API for: {coin_ids}")
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Conection error with API: {e}")
            return {}
        
    def save_raw_data(self, data: Dict[str, Any], prefix: str = "market_data"):
        """
        Save the raw JSON.
        Structure: data/bronze/market/YYYY-MM-DD/prefix_TIMESTAMP.json
        """
        if not data:
            logger.warning("No new data to save.")
            return

        # Define date partition (Partitioning)
        today = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%H%M%S")
        
        # Create dynamic path
        target_dir = self.base_path / today
        target_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"{prefix}_{timestamp}.json"
        file_path = target_dir / filename

        # Save
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            logger.info(f"Data successfully saved in: {file_path}")
        except IOError as e:
            logger.error(f"Failure to write to disk: {e}")

if __name__ == "__main__":
    ingestor = CoinGeckoIngestor()
    
    coins = ["bitcoin", "ethereum", "solana"]
    
    # Extract
    raw_data = ingestor.fetch_current_price(coins)
    
    # Load
    ingestor.save_raw_data(raw_data)