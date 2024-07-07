from pyspark.sql import SparkSession
from datetime import datetime


class Ingestion:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ingest_file(self):
        input_path = "../input/interaction_data.csv"
        output_path = "../output"
        current_date = datetime.now().strftime('%Y-%m-%d')
        input_data = self.spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

        # Write the input data to the current date in stage location.
        input_data.write.mode("append").option("header", "true").csv(f"{output_path}/stage/interactions/{current_date}/")
