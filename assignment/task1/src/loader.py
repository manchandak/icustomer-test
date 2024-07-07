from pyspark.sql import DataFrame


class SqlWriter:
    def __init__(self):
        pass

    def writer(self, df: DataFrame):
        jdbc_url = "jdbc:postgresql://your_host:your_port/your_database"
        table_name = "interactions"
        mode = "append"

        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", "your_username") \
            .option("password", "your_password") \
            .mode(mode) \
            .save()
