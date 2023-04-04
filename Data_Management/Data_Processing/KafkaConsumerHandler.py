import boto3
from kafka import KafkaConsumer

class KafkaConsumerHandler:
    def __init__(self, cluster_arn, topic_name, group_id='my-group'):
        self.cluster_arn = cluster_arn
        self.topic_name = topic_name
        self.group_id = group_id
        
        self.msk_client = boto3.client('kafka')
        
    def _get_bootstrap_servers(self):
        cluster_info = self.msk_client.describe_cluster(ClusterArn=self.cluster_arn)
        bootstrap_servers = ','.join(cluster_info['ClusterInfo']['BrokerNodeGroupInfo']['ClientSubnet'])
        return bootstrap_servers
        
    def create_kafka_consumer(self):
        bootstrap_servers = self._get_bootstrap_servers()
        consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest'
        )
        return consumer
        
    def process_messages(self, message_handler):
        consumer = self.create_kafka_consumer()
        for message in consumer:
            message_handler(message.value)