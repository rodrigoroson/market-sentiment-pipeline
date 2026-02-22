import os
from pathlib import Path
from pyspark.sql import SparkSession
from src.utils.logger import get_logger

logger = get_logger(Path(__file__).stem)

class GoldProcessor:
    def __init__(self):
        logger.info("Starting Spark engine for Gold Layer...")
        self.spark = SparkSession.builder \
            .appName("MarketSentiment_GoldLayer") \
            .master("local[*]") \
            .getOrCreate()
            
        self.silver_market_path = Path("data/silver/market")
        self.silver_news_path = Path("data/silver/news")
        self.gold_output_path = Path("data/gold/daily_metrics")

        # Check if directory exist
        self.gold_output_path.mkdir(parents=True, exist_ok=True)

    def build_business_metrics(self):
        """Combine prices and sentiment using SQL to create the final analytical table.."""
        logger.info("Loading clean data from the Silver Layer...")
        
        try:
            # Read ultra-fast Parquet files
            df_market = self.spark.read.parquet(self.silver_market_path.as_posix())
            df_news = self.spark.read.parquet(self.silver_news_path.as_posix())
        except Exception as e:
            logger.error(f"Data is missing in the Silver Layer. Run the ingestion first. Error: {e}")
            return

        # Expose DataFrames as virtual SQL tables in RAM
        df_market.createOrReplaceTempView("silver_market")
        df_news.createOrReplaceTempView("silver_news")

        logger.info("Performing analytical join with SQL...")

        # We use DATE() to collapse the hours/minutes and have a daily summary
        gold_query = """
        WITH daily_sentiment AS (
            SELECT 
                DATE(timestamp) AS calc_date,
                AVG(sentiment_score) AS avg_daily_sentiment,
                COUNT(*) AS news_volume
            FROM silver_news
            GROUP BY DATE(timestamp)
        ),
        daily_market AS (
            SELECT 
                DATE(timestamp) AS calc_date,
                coin_id,
                AVG(price_usd) AS avg_price,
                MAX(volume_24h) AS peak_volume
            FROM silver_market
            GROUP BY DATE(timestamp), coin_id
        )
        SELECT 
            m.calc_date,
            m.coin_id,
            ROUND(m.avg_price, 2) AS price_usd,
            m.peak_volume,
            COALESCE(ROUND(s.avg_daily_sentiment, 4), 0.0) AS sentiment_index,
            COALESCE(s.news_volume, 0) AS news_count
        FROM daily_market m
        LEFT JOIN daily_sentiment s 
            ON m.calc_date = s.calc_date
        ORDER BY m.calc_date DESC, m.coin_id ASC
        """

        # Execute the query and obtain the final matrix
        df_gold = self.spark.sql(gold_query)
        
        logger.info("Gold table generated. Sample of results:")
        df_gold.show(truncate=False)

        # Persist to disk
        logger.info("Saving consolidated metrics in Capa Gold...")
        df_gold.write.mode("overwrite").parquet(self.gold_output_path.as_posix())
        
        # Also save a CSV file so you can easily open it in Excel.
        csv_path = self.gold_output_path.with_name(f"{self.gold_output_path.name}_csv")
        df_gold.write.mode("overwrite").option("header", "true").csv(csv_path.as_posix())
        
        logger.info("Analytical pipeline successfully completed.")

    def stop(self):
            self.spark.stop()

if __name__ == "__main__":
    processor = GoldProcessor()
    processor.build_business_metrics()
    processor.stop()