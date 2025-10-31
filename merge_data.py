# merge_data.py
import pandas as pd
import os

# File paths
historical_path = "data/crypto_daily_binance.csv"
live_eth_path   = "data/live_dataethusdt.csv"
live_btc_path   = "data/live_databtcusdt.csv"

mergedBTCETH_file = "data/livemarketETHBTC.csv"   # Live BTC + ETH
mergedBTC_file    = "data/BTCUSDT.csv"            # Historical + Live BTC

def safe_read(path):
    """Read CSV safely; return empty DataFrame if file missing."""
    if not os.path.exists(path):
        print(f"⚠️ Missing file: {path}")
        return pd.DataFrame()
    return pd.read_csv(path)

# Read data
historical = safe_read(historical_path)
liveeth    = safe_read(live_eth_path)
livebtc    = safe_read(live_btc_path)

if historical.empty or liveeth.empty or livebtc.empty:
    print("❌ Not enough data to merge. Run fetch_live.py first.")
    exit()

# Filter historical BTC only
historicalbtc = historical[historical["pair"] == "BTCUSDT"]

# Normalize and convert datetime
for df in [historicalbtc, livebtc, liveeth]:
    df.columns = df.columns.str.strip().str.lower()
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")

# Merge live BTC + ETH
mergedBTCETH = pd.concat([liveeth, livebtc], ignore_index=True)
mergedBTCETH = mergedBTCETH.sort_values("datetime").drop_duplicates()

# Merge historical + live BTC
mergedBTC = pd.concat([historicalbtc, livebtc], ignore_index=True)
mergedBTC = mergedBTC.sort_values("datetime").drop_duplicates()

# Save
os.makedirs("data", exist_ok=True)
mergedBTCETH.to_csv(mergedBTCETH_file, index=False)
mergedBTC.to_csv(mergedBTC_file, index=False)

print("✅ Saved merged files successfully:")
print(f"   • Live BTC + ETH → {mergedBTCETH_file}")
print(f"   • Historical + Live BTC → {mergedBTC_file}")
