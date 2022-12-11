import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, Column, MetaData, Float, String, Integer
load_dotenv()
# Extract
df = pd.read_csv('./dato.csv')

# Transformations
mydict = {'Bueno':3,'Excelente':4,'Malo':1,'Aceptable':2}

df['Critic_Score_Class'] = df['Critic_Score_Class'].apply(lambda x : mydict[x] if x == x else  x )

# Create a new MetaData object
metadata = MetaData()

# Create a new table in the database
table = Table('VideoGameSales', metadata,
Column('Platform', String),
Column("Genre", String),
Column('Publisher', String),
Column('NA_Sales', Float),
Column('EU_Sales', Float),
Column('JP_Sales', Float),
Column('Other_Sales', Float),
Column('Global_Sales', Float),
Column('Rating', String),
Column('Critic_Score_Class', Integer))

HOST = os.environ["HOST"]
PORT = os.environ["PORt"]
USER = os.environ["USER"]
PASS = os.environ["PASS"]
DATABASE = os.environ["DATABASE"]
SSLMODE = os.environ["SSLMODE"]
connection_string = f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DATABASE}?sslmode={SSLMODE}"

# Create the engine
engine = create_engine(connection_string)

# Use the engine to create the table in the database
table.create(engine)
# Load
data = df.to_dict()
table.insert().execute(*data)

