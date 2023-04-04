import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, mean

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)

input_data_path = config.get('spark_job', 'input_data')
output_data_path = config.get('spark_job', 'output_data')

def process_data(spark):
    # Read data from S3
    input_data = spark.read.json(input_data_path)

    # Perform data processing and analysis tasks
    avg_premium_by_company = input_data.groupBy("company_name").agg(mean("premium").alias("average_premium"))
    high_risk_drivers = input_data.filter(col("driving_records") == "high-risk")
    missing_credit_scores = input_data.filter(col("credit_score").isNull()).agg(count("*").alias("missing_credit_scores"))

    # Save the results to S3
    avg_premium_by_company.write.json(os.path.join(output_data_path, "avg_premium_by_company.json"))
    high_risk_drivers.write.json(os.path.join(output_data_path, "high_risk_drivers.json"))
    missing_credit_scores.write.json(os.path.join(output_data_path, "missing_credit_scores.json"))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("InsuranceDataProcessing").getOrCreate()
    process_data(spark)
    spark.stop()
    
