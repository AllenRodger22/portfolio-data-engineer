import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, Column, MetaData, Float, String, Integer
from classes import ETL, Connector

metadata = MetaData()

# Create table
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
#start ETL Object
etl = ETL()

connector = Connector(os.environ["HOST"],os.environ["PORT"],os.environ["USER"],os.environ["PASS"],os.environ["DATABASE"],os.environ["SSLMODE"])
engine = connector.createEngine()

data = etl.extract("/dato.csv")
etl.transform()

table.create(engine)

etl.load(data,table)


