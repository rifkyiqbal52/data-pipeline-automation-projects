from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

def transform_data(file_path):
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.csv(file_path, header=True, inferSchema=True, quote='"', escape='"', multiLine=True, encoding="UTF-8")
    
    # Hapus data duplikasi
    df_cleaned = data.dropDuplicates()
    df_cleaned.write.csv('/opt/airflow/data/P2M3_RIFKY-IQBAL_transform', header=True, mode='overwrite')
    return df_cleaned

transform_data('/opt/airflow/data/P2M3_RIFKY-IQBAL_raw')