import configparser
import os
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import IntegerType


"""
This class, InsuranceCollaborativeFiltering, reads the input data, preprocesses it,
trains an ALS model, makes recommendations for all users, 
and writes the recommendations to the specified output path. It uses the configparser library to read input and output data paths from a configuration file. 
"""


import configparser
import os
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import IntegerType

class InsuranceCollaborativeFiltering:
    def __init__(self):
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)

        self.input_data_path = config.get('data', 'input_data_path')
        self.output_data_path = config.get('data', 'output_data_path')
        self.model_path=config.get('model', 'path')

        self.spark = SparkSession.builder.appName("InsuranceCollaborativeFiltering").getOrCreate()

    def read_data(self):
        self.data = self.spark.read.json(self.input_data_path)

    def preprocess_data(self):
        self.data = self.data.withColumn("policyholder_id", self.data["policyholder_id"].cast(IntegerType()))
        self.data = self.data.withColumn("insurance_product_id", self.data["insurance_product_id"].cast(IntegerType()))
        self.data = self.data.withColumn("rating", self.data["rating"].cast(IntegerType()))

    def train_model(self):
        als = ALS(rank=10, maxIter=10, regParam=0.1, userCol="policyholder_id", itemCol="insurance_product_id", ratingCol="rating")
        self.model = als.fit(self.data)

    def make_recommendations(self):
        self.recommendations = self.model.recommendForAllUsers(10)

    def write_recommendations(self):
        self.recommendations.write.json(self.output_data_path)
        
    def save_model(self):
        self.model.save(self.model_path)

    def run(self):
        self.read_data()
        self.preprocess_data()
        self.train_model()
        self.make_recommendations()
        self.write_recommendations()
        self.spark.stop()
        self.save_model()
        


if __name__ == "__main__":
    insurance_cf = InsuranceCollaborativeFiltering()
    insurance_cf.run()
