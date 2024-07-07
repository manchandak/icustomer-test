from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, desc
from datetime import datetime


class Transformer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.input_path = f"../output/clean/{current_date}/clean_zone/"


    def _read_df(self):
        # Load Parquet file into DataFrame with permissive mode
        self.df = self.spark.read.parquet(self.input_path)

    def transform_data(self) -> DataFrame:
        self._read_df()
        # Calculate interaction counts per user_id and product_id
        interaction_count = self.df.groupby("user_id", "product_id").\
            agg(count("interaction_id").alias("interaction_count"))
        return interaction_count.orderBy(desc("interaction_count"))
