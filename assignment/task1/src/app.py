from pyspark.sql import SparkSession, DataFrame
from ingestion import Ingestion
from dataCleaning import Cleaning
from tranformation import Transformer
from loader import SqlWriter

spark = SparkSession.builder.master("local[*]").appName("icustomer test").getOrCreate()

# Data Ingestion
ingest = Ingestion(spark)
ingest.ingest_file()

# Data Cleaning
cleaning = Cleaning(spark)
cleaning.data_cleaning()

# Data Transformation
transform = Transformer(spark)
required_data: DataFrame = transform.transform_data()

# Data Loading
load = SqlWriter()
load.writer(required_data)

spark.stop()
