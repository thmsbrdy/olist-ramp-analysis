import pandas as pd # Importing the pandas library to handle data manipulation and analysis
import psycopg2 # Importing the psycopg2 library to connect to a PostgreSQL database
from sqlalchemy import create_engine # Importing the create_engine function from SQLAlchemy to create a connection to the database
import os # Importing the os library to interact with the operating system, particularly for changing the current working directory

os.chdir(os.path.dirname(os.path.abspath(__file__))) # Changing the current working directory to the directory where the script is located, ensuring that file paths are relative to this location

sellers = pd.read_csv("data/olist_sellers_dataset.csv") # Reading the sellers dataset from a CSV file and storing it in a DataFrame called 'sellers'
orders = pd.read_csv("data/olist_orders_dataset.csv") # Reading the orders dataset from a CSV file and storing it in a DataFrame called 'orders'
order_items = pd.read_csv("data/olist_order_items_dataset.csv") # Reading the order items dataset from a CSV file and storing it in a DataFrame called 'order_items'

engine = create_engine("postgresql://thomasbrady@localhost/olist") # Creating a connection to the PostgreSQL database 

sellers.to_sql("sellers", engine, if_exists="replace", index=False) # Writing the 'sellers' DataFrame to a SQL table named 'sellers' in the database, replacing it if it already exists and not including the index
orders.to_sql("orders", engine, if_exists="replace", index=False) # Writing the 'orders' DataFrame to a SQL table named 'orders' in the database, replacing it if it already exists and not including the index
order_items.to_sql("order_items", engine, if_exists="replace", index=False) # Writing the 'order_items' DataFrame to a SQL table named 'order_items' in the database, replacing it if it already exists and not including the index 
