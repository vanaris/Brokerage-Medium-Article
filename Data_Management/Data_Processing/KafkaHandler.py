import boto3
from kafka import KafkaProducer


""" 
This class can be used to manage your Kafka cluster on Amazon MSK, create topics, send messages to a topic,
and close the Kafka producer when done. It uses the kafka-python library to interact with the Kafka cluster.
"""

class KafkaHandler:
    def __init__(self, cluster_arn, topic_name):
        self.cluster_arn = cluster_arn
        self.topic_name = topic_name
        self.kafka_producer = None
        
        self.msk_client = boto3.client('kafka')
        
    def _get_bootstrap_servers(self):
        cluster_info = self.msk_client.describe_cluster(ClusterArn=self.cluster_arn)
        bootstrap_servers = ','.join(cluster_info['ClusterInfo']['BrokerNodeGroupInfo']['ClientSubnet'])
        return bootstrap_servers
        
    def create_topic(self, num_partitions=1, replication_factor=1):
        bootstrap_servers = self._get_bootstrap_servers()
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        producer.send('create_topic', {'topic': self.topic_name, 'partitions': num_partitions, 'replication_factor': replication_factor})
        
    def setup_kafka_producer(self):
        bootstrap_servers = self._get_bootstrap_servers()
        self.kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        
    def send_message(self, message):
        if self.kafka_producer is None:
            self.setup_kafka_producer()
        self.kafka_producer.send(self.topic_name, message)
        
    def close_producer(self):
        if self.kafka_producer is not None:
            self.kafka_producer.close()