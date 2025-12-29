import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv 

load_dotenv() 

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS") 
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "personal_analytics")

if not DB_PASS:
    raise ValueError("DB_PASS not found in .env file!")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

tickers = [
    'AMZN', 'ASML', 'AVGO', 'GOOGL', 'META', 
    'MSFT', 'NBIS', 'NVDA', 'PLTR', 'TSLA'
]

def fetch_and_load_data():
    print("--- Starting Data Fetch ---")
    
    success_count = 0
    
    for ticker in tickers:
        print(f"Fetching {ticker}...")
        
        try:
            # 1. Fetch 5 years of data
            df = yf.download(ticker, period="5y", auto_adjust=True, progress=False)
            
            if df.empty:
                print(f"[WARN] No data found for {ticker}")
                continue

            # If yfinance returns MultiIndex columns (e.g. ('Close', 'AMZN')), flatten them
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            # -----------------------------

            # Reset index so 'Date' becomes a col
            df.reset_index(inplace=True)
            
            # Standardize col names
            df.columns = [str(c).lower().replace(' ', '_') for c in df.columns]
            
            # 2. Drop the table with CASCADE
            table_name = f"{ticker.lower()}_stock_prices"
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS raw.{table_name} CASCADE"))
                conn.commit()

            # 3. Save to Postgres
            df.to_sql(
                name=table_name,
                con=engine,
                schema="raw",
                if_exists="replace",
                index=False
            )
            print(f"[OK] Loaded {ticker} ({len(df)} rows)")
            success_count += 1
            
        except Exception as e:
            print(f"[ERROR] Failed to process {ticker}: {e}")
            raise e

    if success_count == 0:
        raise Exception("No data was loaded! Check connection or tickers.")

if __name__ == "__main__":
    fetch_and_load_data()