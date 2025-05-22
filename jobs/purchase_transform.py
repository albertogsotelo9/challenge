import argparse
import logging
from pyspark.sql import SparkSession
from src.data_processing import process_data


logging.basicConfig(
    level=logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)
def main(input_path: str, output_path: str) -> None:
    spark = SparkSession.builder.appName("meli_challenge").getOrCreate()
    logger.info("Spark session started")

    logger.info(f"Reading data from: {input_path}")
    df = (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(input_path))

    logger.info("Starting data transformation")
    result = process_data(df)

    logger.info(f"Writing output to: {output_path}")
    result.write.mode("overwrite").parquet(output_path)

    logger.info("Job completed successfully")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input path to CSV")
    parser.add_argument("--output", required=True, help="Output path to Parquet")
    args = parser.parse_args()


    try:
        main(args.input, args.output)
    except Exception as e:
        logger.exception("Job failed due to an error")
        raise  
    