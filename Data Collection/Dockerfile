# Use the official Apache Spark base image
FROM apache/spark:3.2.0

# Install additional dependencies, if needed
# RUN apt-get update && apt-get install -y ...

# Copy Spark application files to the Docker image
COPY spark_app.py /app/

# Set the working directory
WORKDIR /app

# Set the entry point for the Docker container
ENTRYPOINT ["/bin/bash", "-c", "spark-submit --master local[*] spark_app.py"]
