# fetch_live.py
from binance.client import Client
import pandas as pd
import time
import os

client = Client()

# Settings
pairs = ["BTCUSDT", "ETHUSDT"]

def fetch_latest_data(pair="BTCUSDT", interval="1m", limit=60):
    """Fetch recent 1-minute candle data from Binance."""
    klines = client.get_klines(symbol=pair, interval=interval, limit=limit)
    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "num_trades", "taker_base_vol", "taker_quote_vol", "ignore"
    ])
    df["pair"] = pair
    df["datetime"] = pd.to_datetime(df["close_time"], unit="ms")
    df["close"] = df["close"].astype(float)
    return df[["datetime", "pair", "close"]]

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    print("🚀 Starting live data fetch loop (Ctrl + C to stop)")
    last_update = {}

    while True:
        for sym in pairs:
            try:
                live_data = fetch_latest_data(sym)
                filename = f"data/live_data{sym.lower()}.csv"
                live_data.to_csv(filename, index=False)

                latest_time = live_data["datetime"].iloc[-1]
                if sym not in last_update or latest_time > last_update[sym]:
                    print(f"✅ Updated {sym} at {latest_time}")
                    last_update[sym] = latest_time
            except Exception as e:
                print(f"⚠️ Error updating {sym}: {e}")

        time.sleep(60)  # wait 1 minute before fetching again
