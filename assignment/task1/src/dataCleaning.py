from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, TimestampType


class Cleaning:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schema = StructType([
            StructField("interaction_id", IntegerType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("action", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("_corrupt_record", StringType(), True)
        ])
        self.current_date = datetime.now().strftime('%Y-%m-%d')
        self.inputPath = f"../output/stage/interactions/{self.current_date}/"

    def _read_df(self):
        # Load CSV file into DataFrame with permissive mode
        self.interactions_data = self.spark.read.option("mode", "PERMISSIVE").\
            csv(self.inputPath, header=True, schema=self.schema)
        self.interactions_data.persist()

    def _validate_data(self):
        # Separate valid and invalid records
        invalid_schema_df = self.interactions_data.filter(col("_corrupt_record").isNotNull())
        valid_df = self.interactions_data.filter((col("_corrupt_record").isNull()) | (lower(col("_corrupt_record")) == 'null'))

        # Step 2: interaction_id, user_id should not be null
        valid_df = valid_df.filter(col("interaction_id").isNotNull() & col("user_id").isNotNull())

        invalid_df = self.interactions_data.filter(col("interaction_id").isNull() | col("user_id").isNull())

        # Step 3: Filter product_id and action which are null
        valid_df = valid_df.filter(col("product_id").isNotNull() & col("action").isNotNull())
        invalid_df = invalid_df.union(valid_df.filter(col("product_id").isNull() | col("action").isNull()))

        # Step 5: interaction_id, user_id cannot be zero
        invalid_zero_values_df = valid_df.filter((col("interaction_id") == 0) | (col("user_id") == 0))
        valid_df = valid_df.filter((col("interaction_id") != 0) & (col("user_id") != 0))
        invalid_df = invalid_df.union(invalid_zero_values_df)

        # Step 6: interaction_id should be unique and non-null
        self.valid_df = valid_df.dropDuplicates(["interaction_id"])

        # Combine all invalid records
        self.invalid_df = invalid_df.union(invalid_schema_df)

    def _write_valid_invalid(self):
        # Write invalid records to a separate directory
        invalid_path = f"../output/clean/{self.current_date}/invalid_data/"
        self.invalid_df.write.parquet(invalid_path, mode="overwrite")

        # Write valid records for further processing
        valid_path = f"../output/clean/{self.current_date}/clean_zone/"
        self.valid_df.write.parquet(valid_path, mode="overwrite")

    def data_cleaning(self):
        self._read_df()
        self._validate_data()
        self._write_valid_invalid()

