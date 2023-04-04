from pyspark.sql import SparkSession
import os
import configparser
from pyspark.sql.functions import col, avg, when

def process_data(spark):
    # Read configuration file
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)

    # Read data from S3
    input_data_path = config.get('spark_job', 'customer_data')
    input_data = spark.read.json(input_data_path)

    # Perform data processing and analysis tasks

    # Calculate the average premium for each insurance company
    average_premiums = input_data.groupBy('insurance_company').agg(avg('premium').alias('average_premium'))

    # Calculate the average claim amount for high-risk drivers
    high_risk_drivers = input_data.filter(col('risk_level') == 'high')
    average_claim_amount = high_risk_drivers.agg(avg('claim_amount').alias('average_claim_amount'))

    # Save the results to S3
    output_data_path = config.get('spark_job', 'analysis_results')
    average_premiums.write.json(os.path.join(output_data_path, 'average_premiums'))
    average_claim_amount.write.json(os.path.join(output_data_path, 'average_claim_amount'))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("InsuranceDataProcessing").getOrCreate()
    process_data(spark)
    spark.stop()