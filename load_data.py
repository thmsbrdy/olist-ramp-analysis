import pandas as pd 
from sqlalchemy import create_engine 
import os 

# ensure relatives paths work regardless of where the script is run from
os.chdir(os.path.dirname(os.path.abspath(__file__))) 

# read raw CSVs into memory
sellers = pd.read_csv("data/olist_sellers_dataset.csv") 
orders = pd.read_csv("data/olist_orders_dataset.csv") 
order_items = pd.read_csv("data/olist_order_items_dataset.csv") 

# connect to local Postgres database
engine = create_engine("postgresql://thomasbrady@localhost/olist") 

# write each DataFrame to a SQL table, replacing if it already exists
sellers.to_sql("sellers", engine, if_exists="replace", index=False)
orders.to_sql("orders", engine, if_exists="replace", index=False)
order_items.to_sql("order_items", engine, if_exists="replace", index=False)