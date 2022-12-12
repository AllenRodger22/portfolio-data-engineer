from sqlalchemy import create_engine
import pandas as pd

class ETL():
    def __init__(self):
        self.self = self
        self.mydict = {'Bueno':3,'Excelente':4,'Malo':1,'Aceptable':2}

    def extract(self,data_path):
        df = pd.read_csv(data_path)
        return df

    def transform(self,data):
        data['Critic_Score_Class'] = data['Critic_Score_Class'].apply(lambda x : self.mydict[x] if x == x else  x )

    def load(self,data,target_table):
        target_table.insert().execute(*data.to_dict())

class Connector():
    def __init__(self,host,port,user,password,database,sslmode):
        self.self = self
        self.host = host 
        self.port = port 
        self.user = user
        self.password = password
        self.database = database
        self.sslmode = sslmode
    
    def createEngine(self):
        CONNECTION_STRING = f"postgresql://{self.host}:{self.password}@{self.host}:{self.port}/{self.user}?sslmode={self.sslmode}"
        return create_engine(CONNECTION_STRING)
