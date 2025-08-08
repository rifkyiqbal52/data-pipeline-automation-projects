from pyspark.sql import SparkSession

def load_data(file_path):
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.csv(file_path, header=True, inferSchema=True, quote='"', escape='"', multiLine=True, encoding="UTF-8")
    data.write.csv('/opt/airflow/data/P2M3_RIFKY-IQBAL_raw', header=True, mode='overwrite')
    return data

load_data("/opt/airflow/data/P2M3_Rifky-Iqbal_data_raw.csv")
