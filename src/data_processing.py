from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def process_data(df: DataFrame) -> DataFrame:
    
    logger.info("Starting transformation process")
    return (
        df.dropna(subset=["user_id", "email", "purchase_datetime"])
          .withColumn("purchase_datetime", F.to_timestamp("purchase_datetime"))
          .withColumn("purchase_amount", F.col("purchase_amount").cast("float"))
          .filter((F.col("purchase_datetime") >= "2023-01-01") &
                  (F.col("purchase_datetime") <= "2023-12-31"))
          .groupBy("user_id")
          .agg(F.sum("purchase_amount").alias("total_purchase_amount"))
    )

