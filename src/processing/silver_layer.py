import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, to_timestamp
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from textblob import TextBlob
from src.utils.nlp_setup import setup_nlp_environment
from src.utils.logger import get_logger

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

logger = get_logger(__name__)

class SilverProcessor:
    def __init__(self):
        # Initialize the processing engine
        logger.info("Starting Spark Session...")
        self.spark = SparkSession.builder \
            .appName("MarketSentiment_SilverLayer") \
            .master("local[*]") \
            .getOrCreate()
            
        # Entry (Bronze) and exit (Silver) routes
        self.bronze_market_path = Path("data/bronze/market/*/*.json")
        self.silver_market_path = Path("data/silver/market")
        self.bronze_news_path = Path("data/bronze/news/*/*.json")
        self.silver_news_path = Path("data/silver/news")

    def process_market_data(self):
        """It reads the raw JSON from CoinGecko, flattens it, and types it."""
        logger.info("Processing market data...")

        df_raw = self.spark.read.option("multiline", "true").json(self.bronze_market_path.as_posix())

        if df_raw.rdd.isEmpty():
            logger.warning("No data was found in the Bronze Layer.")
            return
        
        # Data wrangling
        if "bitcoin" in df_raw.columns:
            df_clean = df_raw.select(
                col("bitcoin.usd").alias("price_usd"),
                col("bitcoin.usd_market_cap").alias("market_cap"),
                col("bitcoin.usd_24h_vol").alias("volume_24h"),
                col("bitcoin.last_updated_at").alias("last_updated_ts")
            )
            
            # Casting: Convert the UNIX timestamp (seconds) to a standard UTC Date/Time format
            df_clean = df_clean.withColumn(
                "timestamp", 
                to_timestamp(col("last_updated_ts"))
            ).drop("last_updated_ts")
            
            # AAdd metadata about when it was processed in Silver
            df_clean = df_clean.withColumn("processed_at", current_timestamp())

            # Show the resulting mathematical scheme and a couple of rows
            df_clean.printSchema()
            df_clean.show(truncate=False)

            # Load
            logger.info("Writing clean data in Parquet format...")
            self.silver_market_path.mkdir(parents=True, exist_ok=True)
            df_clean.write.mode("append").parquet(self.silver_market_path.as_posix())
            logger.info("Market processing completed.")

    def process_news_data(self):
        """Read the raw headlines, clean up the text, and quantify the sentiment.."""
        logger.info("Processing news data...")
        
        # Read news JSON
        df_raw = self.spark.read.option("multiline", "true").json(self.bronze_news_path.as_posix())
        
        if df_raw.rdd.isEmpty():
            logger.warning("No news was found in the Bronze Layer.")
            return

        # Define the mathematical model (UDF: User Defined Function)
        def get_sentiment(text):
            try:
                # TextBlob returns a polarity of -1.0 to 1.0
                return float(TextBlob(text).sentiment.polarity)
            except Exception:
                return 0.0 # Neutral in case of error

        # Wrap the native Python function in a Spar-compatible wrapper
        sentiment_udf = udf(get_sentiment, FloatType())

        # Transformation and Cleaning
        logger.info("Applying NLP sentiment analysis and cleansing...")
        df_clean = df_raw.select(
            col("source"),
            col("title"),
            col("scraped_at")
        )
        
        # Casting: Convert the UNIX timestamp (seconds) to a standard UTC Date/Time format
        df_clean = df_clean.withColumn("timestamp", to_timestamp(col("scraped_at"))).drop("scraped_at")
        
        # Calculate the sentiment score by creating a new column
        df_clean = df_clean.withColumn("sentiment_score", sentiment_udf(col("title")))
        df_clean = df_clean.withColumn("processed_at", current_timestamp())

        # Show results
        df_clean.printSchema()
        df_clean.show(truncate=False)

        # Load
        logger.info("Writing structured news in Parquet format...")
        df_clean.write.mode("append").parquet(self.silver_news_path.as_posix())
        logger.info("News processing completed.")
    
    def stop(self):
        """Turn off the Spark engine to free up RAM."""
        self.spark.stop()

if __name__ == "__main__":
    setup_nlp_environment()

    processor = SilverProcessor()
    processor.process_market_data()
    processor.process_news_data()
    processor.stop()