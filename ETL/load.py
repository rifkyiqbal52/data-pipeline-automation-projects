import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession

def save_to_mongodb(file_path, db_name, collection_name, mongo_uri):
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.csv(file_path, header=True, inferSchema=True, quote='"', escape='"', multiLine=True, encoding="UTF-8")
    
    # Convert PySpark DataFrame to Pandas DataFrame
    pandas_df = data.toPandas()
    pandas_df.dtypes
    pandas_df['Date_of_Admission']=pd.to_datetime(pandas_df['Date_of_Admission'])
    pandas_df['Discharge_Date']=pd.to_datetime(pandas_df['Discharge_Date'])
    print(pandas_df.dtypes)

    # Convert to dictionary format
    data_dict = pandas_df.to_dict("records")

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Insert data
    collection.insert_many(data_dict)

save_to_mongodb('/opt/airflow/data/P2M3_RIFKY-IQBAL_transform', 'Milestone3', 'P2M3_Rifky-Iqbal', 'LINK MONGO URI')