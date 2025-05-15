import pandas as pd
import os
import sys
from datetime import datetime

def map_exchange(val):
    val = str(val).upper()
    if any(x in val for x in ["NIFTY", "FINNIFTY", "BANKNIFTY"]):
        return "NSEFNO"
    elif "SENSEX" in val:
        return "BSEFNO"
    return None


def transform_csv(input_path):

    today_str = datetime.today().strftime('%d%m%Y')

    if not os.path.exists(input_path):
        print(f"File '{input_path}' does not exist.")
        sys.exit(1)

    # refdata_path = f"/home/zanskar/titan_data/refdata/refdata_{today_str}.csv"
    refdata_path = f"refdata_{today_str}.csv"
    if not os.path.exists(refdata_path):
        print(f"Refdata file '{refdata_path}' does not exist.")
        sys.exit(1)

    try:
        df = pd.read_csv(input_path, skiprows=None, header=None)
        print(f"Read {len(df)} rows from {input_path}")
    except Exception as e:
        print(f"Failed to read CSV: {e}")
        sys.exit(1)

    try:
        # Load reference close price CSV
        ref_df = pd.read_csv(refdata_path)
        if "TICKER" not in ref_df.columns or "PREV_CLOSE" not in ref_df.columns:
            print("Refdata CSV must contain 'TICKER' and 'PREV_CLOSE' columns.")
            sys.exit(1)

        # Create dictionary for fast lookup
        close_price_map = dict(zip(ref_df["TICKER"], ref_df["PREV_CLOSE"]))

        # Construct new DataFrame
        new_df = pd.DataFrame()
        new_df["CODE"] = ["OWN"] * len(df)
        new_df["TICKER"] = df[0]
        new_df["EXCHANGE"] = new_df["TICKER"].apply(map_exchange)
        new_df["BUYQTY"] = df[1]
        new_df["BUYVALUE"] = df[2]
        new_df["SELLQTY"] = df[3]
        new_df["SELLVALUE"] = df[4]
        new_df["CARRYQTY"] = df[1] + df[3] + df[5]
        new_df["CARRYVALUE"] = (df[2] + df[4] + df[6])
        new_df["NETQTY"] = new_df["CARRYQTY"]
        new_df["CLOSPRICE"] = new_df["TICKER"].map(close_price_map)

        # Save to CSV
        output_path = f"CUSTOM_POSITION_{today_str}.csv"
        new_df.to_csv(output_path, index=False)
        print(f"Transformed CSV written to: {output_path}")

    except Exception as e:
        print(f"Transformation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    transform_csv("sample_positions.csv")
