from binance.client import Client
import pandas as pd
from datetime import datetime
import time
import os
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

client = Client()

# Settings
pairs = ["BTCUSDT", "ETHUSDT"]
interval = Client.KLINE_INTERVAL_1HOUR  # 1-hour candles
start_str = datetime(2025, 1, 1).strftime("%Y-%m-%d %H:%M:%S")

frames = []

for sym in pairs:
    print(f"📥 Fetching historical data for {sym}...")
    klines = client.get_historical_klines(symbol=sym, interval=interval, start_str=start_str)
    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume", "close_time",
        "qav", "num_trades", "taker_base_vol", "taker_quote_vol", "ignore"
    ])
    df["pair"] = sym
    df["datetime"] = pd.to_datetime(df["close_time"], unit="ms")
    df["close"] = df["close"].astype(float) 
    df = df[["datetime", "pair", "close"]].sort_values("datetime") 
    frames.append(df)

data = pd.concat(frames)

# Convert to daily frequency and compute indicators
data = (
    data
    .set_index("datetime")
    .groupby("pair", group_keys=False) 
    .apply(lambda g: g.resample("1D").last())
    .reset_index()
)

data["return"] = data.groupby("pair")["close"].pct_change()
data["ma7"] = data.groupby("pair")["close"].transform(lambda s: s.rolling(7).mean())
data["ma30"] = data.groupby("pair")["close"].transform(lambda s: s.rolling(30).mean())
data["vol7"] = data.groupby("pair")["return"].transform(lambda s: s.rolling(7).std())

# Save the historical dataset
data.to_csv("data/crypto_daily_binance.csv", index=False)
print("✅ Saved historical data to crypto_daily_binance.csv")

# Live updates 
def fetch_latest_data(pair="BTCUSDT", interval="1m", limit=60):
    """Fetch recent 1-minute candle data from Binance."""
    klines = client.get_klines(symbol=pair, interval=interval, limit=limit) 
    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "num_trades", "taker_base_vol", "taker_quote_vol", "ignore"
    ])
    df["pair"]=pair 
    df["datetime"] = pd.to_datetime(df["close_time"], unit="ms")
    df["close"] = df["close"].astype(float)
    return df[["datetime", "pair", "close"]]

if __name__ == "__main__":
    print("🚀 Starting live data fetch loop (Ctrl + C to stop)")
    while True:
        for sym in pairs:
            live_data = fetch_latest_data(sym)
            filename = f"data/live_data{sym.lower()}.csv"
            live_data.to_csv(filename, index=False)
            print(f"✅ Updated {sym} at {live_data['datetime'].iloc[-1]}")

        #Paths
        live_eth_path = "data/live_dataethusdt.csv"
        live_btc_path = "data/live_databtcusdt.csv"
        merged_path = "data/livemarket.csv"

        #Read both files
        liveeth=pd.read_csv(live_eth_path)
        livebtc=pd.read_csv(live_btc_path)

        #Combine and drop duplicates
        merged= pd.concat([liveeth, livebtc], ignore_index=True)
        merged["datetime"] = pd.to_datetime(merged["datetime"])

        #Save merged files
        os.makedirs("data", exist_ok=True)
        merged.to_csv(merged_path, index=False)

        print(f"Save merged BTC data to {merged_path}")

        # Authenticate once
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()
        drive = GoogleDrive(gauth)
        folder_id = "1Y7pk5VUKTwXp38ZeL2BVVaX9xXrugz8A" 

        
        time.sleep(60)  # fetch new data every 60 seconds
