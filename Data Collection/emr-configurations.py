import os
import json

account_id = os.environ["ACCOUNT_ID"]
ecr_region = os.environ["ECR_REGION"]
ecr_repo_name = os.environ["ECR_REPO_NAME"]

config_json = [
    {
        "Classification": "spark-defaults",
        "Properties": {
            "spark.executorEnv.containerImage": f"{account_id}.dkr.ecr.{ecr_region}.amazonaws.com/{ecr_repo_name}:latest",
            "spark.yarn.appMasterEnv.containerImage": f"{account_id}.dkr.ecr.{ecr_region}.amazonaws.com/{ecr_repo_name}:latest",
            "spark.executor.instances": "2",
            "spark.executor.cores": "2",
            "spark.executor.memory": "4g"
        }
    }
]

config_str = json.dumps(config_json)

# Use the `config_str` variable when submitting your EMR job