from pyspark.sql import SparkSession

class ETL():
    def __init__(self,appName):
        self.self = self
        self.appName = appName
        self.spark = SparkSession.builder.appName(f"{appName}").getOrCreate()

    def extract(self,data_path,t):
        if t == "csv":
            df = self.spark.read.csv(data_path)
            return df
        elif t == "parquet":
            df = self.spark.read.parquet(data_path)
            return df

    def transform(self,df):
        df = df.select("*").where( df["_c2"] == "Nintendo")
        return df
    def load(self,df,target_path):
        df.write.parquet(target_path)

    def stop(self):
        self.spark.stop()