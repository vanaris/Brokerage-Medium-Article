"""
To use Apache Spark with Amazon EMR, first, you need to create an EMR cluster with Spark installed.
Once your cluster is ready, you can submit your Spark application to process data from Kafka.

This is a simple Spark application that reads data from Kafka and performs some processing
This code reads data from the specified Kafka topic, filters messages containing "HighRisk", and writes the results to the console.


"""



import configparser
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_data(spark, kafka_bootstrap_servers, topic_name):
    # Read configuration file

    # Read data from Kafka
    input_data = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic_name)
        .load()
    )

    # Perform data processing and analysis tasks
    # For example, filter messages with high-risk drivers
    high_risk_drivers = input_data.where(col("value").contains("HighRisk"))

    # Write the results to the console (replace this with your desired output location)
    query = (
        high_risk_drivers.writeStream
        .outputMode("append")
        .format("console")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)
    servers=config.get('kafka', 'servers')
    topic=config.get('kafka, topic_name')
    config.get('spark_job', 'input_data')
    spark = SparkSession.builder.appName("InsuranceDataProcessing").getOrCreate()
    kafka_bootstrap_servers = servers
    topic_name = topic
    process_data(spark, kafka_bootstrap_servers, topic_name)
    spark.stop()
