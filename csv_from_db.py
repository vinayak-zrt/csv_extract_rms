import pandas as pd
import logging, json, os
from datetime import datetime
import sys
from sqlalchemy import create_engine
import psycopg2


#read db configs and connect to postgres db
#read from the db for to fetch all columns with date 
#write to csv - non_filtered_trades
#check expiry data and remove contracts which expired - this is done by creating a date format of today - %Y%m%d (20250731). we check if todaydate value is present in TICKER column value for all the rows and then remove them
# write to csv - filtered_trades.csv 

def read_config(input_path):
    if not os.path.exists(input_path):
        print(f"File '{input_path}' does not exist.")
        sys.exit(1)

    with open(input_path, 'r') as f:
        return json.load(f)
    

def create_db_engine(config_data):
    db_url = (
        f"postgresql://{config_data['db_user']}:{config_data['db_pwd']}"
        f"@{config_data['db_host']}:{config_data.get('db_port', 5432)}/{config_data['db_name']}"
    )
    
    return create_engine(db_url, echo=False, future=True)


def fetch_trades(engine, table_name):
    query = f"select * from {table_name}"
    
    return pd.read_sql(query, engine)


def filter_expired_contracts(df, ticker_column="TICKER"):
    today_str = datetime.today().strftime("%Y%m%d")
    
    return df[~df[ticker_column].astype(str).str.contains(today_str)]


def main():

    try:
        config = read_config("db_config.json")
        db_engine = create_db_engine(config)
        today_str = datetime.today().strftime("%Y%m%d")

        df = fetch_trades(db_engine, config['db_table'])
        print(f"Retrieved {len(df)} rows from DB.")

        df.to_csv(f"Non_filtered_trades_{today_str}.csv", index=False)
        print("Saved Non filtered trades")

        filtered_df = filter_expired_contracts(df)
        filtered_df.to_csv(f"Filtered_trades_{today_str}.csv", index=False)
        print("Saved filtered trades")
    
    except Exception as e:
        print(f"Error occured during execution - {e}")

    finally:
        try:
            db_engine.dispose()
            print("Database engine disposed.")
        except Exception:
            pass



if __name__ == "__main__":
    main()