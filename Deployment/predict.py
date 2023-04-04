# predict.py
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession

def load_model(model_path):
    return ALSModel.load(model_path)

def predict(model, user_id):
    recommendations = model.recommendForUserSubset(user_id, 10)
    return recommendations

if __name__ == "__main__":
    spark = SparkSession.builder.appName("InsurancePredictions").getOrCreate()
    model_path = "s3://your-data-bucket/models/insurance_als_model"
    model = load_model(model_path)

    user_id = 12345
    recommendations = predict(model, user_id)
    recommendations.show()

    spark.stop()