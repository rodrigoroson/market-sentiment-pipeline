import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(Path(__file__).stem)

def plot_all_trends():
    logger.info("Generating interactive visual panels for all currencies...")
    
    gold_path = Path("data/gold/daily_metrics_csv")
    csv_files = list(gold_path.glob("part-*.csv"))
    
    if not csv_files:
        logger.error("The CSV file was not found in the Gold Layer. Run the pipeline first..")
        return

    # Read the master DataFrame and ensure that the dates are Datetime objects
    df = pd.read_csv(csv_files[0])
    df['calc_date'] = pd.to_datetime(df['calc_date'])
    
    # Read which coins are in the database
    coins_available = df['coin_id'].unique()
    logger.info(f"{len(coins_available)} coins were detected: {', '.join(coins_available)}")

    # Create the output directory if it does not exist
    output_dir = Path("data/gold/visualizations")
    output_dir.mkdir(parents=True, exist_ok=True)

    # One panel per asset
    for coin in coins_available:
        logger.info(f"Rendering a graphic for: {coin.upper()}...")
        
        # Filter the data exclusively for the current currency
        df_coin = df[df['coin_id'] == coin].copy()
        df_coin = df_coin.sort_values('calc_date')

        # Create the canvas
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        fig.suptitle(f'{coin.capitalize()}: Price vs. Sentiment Analysis of News', fontsize=16, fontweight='bold')

        # --- Top Chart: Price Evolution ---
        ax1.plot(df_coin['calc_date'], df_coin['price_usd'], color='#1f77b4', marker='o', linewidth=2, markersize=6)
        ax1.set_ylabel('Price (USD)', fontsize=12, fontweight='bold')
        ax1.grid(True, linestyle='--', alpha=0.7)
        
        # Add price labels in numeric format
        for x, y in zip(df_coin['calc_date'], df_coin['price_usd']):
            # We display 2 decimal places if the price is low (like Solana), or no decimal places if it is high (like Bitcoin).
            price_format = f'${y:,.2f}' if y < 1000 else f'${y:,.0f}'
            ax1.annotate(price_format, xy=(x, y), xytext=(0, 10), textcoords='offset points', ha='center', fontsize=9)

        # --- Bottom Chart: NLP Sentiment Index ---
        colors = ['#2ca02c' if s > 0 else '#d62728' if s < 0 else '#7f7f7f' for s in df_coin['sentiment_index']]
        
        ax2.bar(df_coin['calc_date'], df_coin['sentiment_index'], color=colors, alpha=0.7, width=0.4)
        ax2.set_ylabel('Polarity (-1 a 1)', fontsize=12, fontweight='bold')
        ax2.axhline(0, color='black', linewidth=1.2)
        ax2.grid(True, linestyle='--', alpha=0.3)
        ax2.set_ylim(-1.0, 1.0)

        # Format the dates on the X-axis
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gcf().autofmt_xdate()

        plt.tight_layout()

        # Save visual evidence dynamically
        output_file = output_dir / f"{coin}_trend_dashboard.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        
        # IMPORTANT: Free up RAM by closing the current canvas
        plt.close(fig) 
        
    logger.info("All dashboards have been successfully generated.")
    
if __name__ == "__main__":
    plot_all_trends()