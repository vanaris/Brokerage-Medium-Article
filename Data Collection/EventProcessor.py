import boto3
import json
import configparser
import os
from logger_setup import setup_logger

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')


class EventProcessor(object):
    
    def __init__(self, event, config_path=config_path, **kwargs):
        
        self.event=event
        self.s3= boto3.client("s3")
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.bucket=self.config.get('event', 'bucket')
        self.key=self.config.get('event', 'key')  #ex "processed-data/customer_data.json"
        self.glue_customer=self.config.get('event', 'customer_job')
        self.glue_enrich=self.config.get('event', 'enrich_job')
        self.glue_fraud=self.config.get('event', 'fraud_job')
        self.glue_reccomend=self.config.get('event', 'recommend_job')
        self.glue_process= self.config.get('event', 'process')
        self.logger = setup_logger(__name__, 'Setup.log')  # Setup logger using the imported function
        self.__dict__.update(kwargs)  # Store all the extra variables
        
        
    def process_event(self):
        event_type = self.event["event_type"]

        if event_type == "process_customer_data":
            self.process_customer_data_event()
        elif event_type == "enrich_customer_data":
            self.enrich_customer_data_event()
        elif event_type == "detect_fraud":
            self.detect_fraud_event()
        elif event_type == "generate_recommendations":
            self.generate_recommendations_event()
        
    def process_customer_data_event(self):
        customer_data = self.event["customer_data"]
        processed_data = self.process_customer_data(customer_data)
        self.store_processed_data(processed_data)
        
        
    def process_customer_data_event(self):
        """ Cleans and transforms raw customer data."""
        customer_data = self.event["customer_data"]
        self.trigger_data_processing_job(customer_data, self.glue_customer)

    def enrich_customer_data_event(self):
        """ Enriches customer data with additional information, such as credit score or driving history."""
        customer_data = self.event["customer_data"]
        self.trigger_data_processing_job(customer_data, self.glue_enrich)

    def detect_fraud_event(self):
        """ Analyzes customer data for potential fraud indicators."""
        customer_data = self.event["customer_data"]
        self.trigger_data_processing_job(customer_data, self.glue_fraud)

    def generate_recommendations_event(self):
        """Generates personalized insurance product recommendations for customers."""
        customer_data = self.event["customer_data"]
        self.trigger_data_processing_job(customer_data, self.glue_reccomend)

        
    def process_customer_data_event(self):
        
        customer_data = self.event["customer_data"]
        self.trigger_data_processing_job(customer_data)

    def trigger_data_processing_job(self, customer_data):
        # Trigger an AWS Glue job or AWS Batch job to process the customer data
        # This example triggers an AWS Glue job
        glue = boto3.client("glue")
        glue.start_job_run(
            JobName=self.glue_process,
            Arguments={
                "--customer_data": json.dumps(customer_data)
            }
        )

    
    def store_processed_data(self, processed_data):
        self.s3.put_object(Bucket=self.bucket, Key=self.key, Body=json.dumps(processed_data))


def lambda_handler(event, context):
    event_processor = EventProcessor(event)
    event_processor.process_event()

    return {"statusCode": 200, "body": "Event processed successfully"}
