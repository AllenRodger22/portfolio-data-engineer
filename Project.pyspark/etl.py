from classes import ETL

etl = ETL("MyEtl")


data = etl.extract("./data source/videogamesales.csv","csv")
transformed_data = etl.transform(data)
etl.load(df = transformed_data, target_path= "Project.pyspark/target_storage/videogamesales")
etl.stop()