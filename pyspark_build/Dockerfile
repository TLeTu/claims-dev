# Use the official Spark Python image as a base
FROM apache/spark-py:v3.4.0

# We switch to the 'root' user to install system-wide dependencies.
USER root

# Set the working directory
WORKDIR /opt/spark

# Copy the requirements file into the container
COPY requirements.txt .

# Install the Python dependencies as root
RUN pip install --no-cache-dir -r requirements.txt

# --- Download Spark Connector Jars ---
# Download connectors for Kafka and Delta Lake compatible with Spark 3.4.0 and Scala 2.12
# and place them in Spark's jars directory so they are automatically on the classpath.
ENV SPARK_KAFKA_VERSION=3.4.0
ENV DELTA_LAKE_VERSION=2.4.0
ENV KAFKA_CLIENTS_VERSION=3.4.0
ENV COMMONS_POOL_VERSION=2.11.1
ENV SCALA_VERSION=2.12
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

# Kafka and Delta Lake Jars
RUN curl -L "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_KAFKA_VERSION}.jar" --output /opt/spark/jars/spark-sql-kafka.jar && \
    curl -L "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" --output /opt/spark/jars/kafka-clients.jar && \
    curl -L "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_KAFKA_VERSION}.jar" --output /opt/spark/jars/spark-token-provider-kafka.jar && \
    curl -L "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar" --output /opt/spark/jars/commons-pool2.jar && \
    curl -L "https://repo1.maven.org/maven2/io/delta/delta-core_${SCALA_VERSION}/${DELTA_LAKE_VERSION}/delta-core_${SCALA_VERSION}-${DELTA_LAKE_VERSION}.jar" --output /opt/spark/jars/delta-core.jar && \
    curl -L "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_LAKE_VERSION}/delta-storage-${DELTA_LAKE_VERSION}.jar" --output /opt/spark/jars/delta-storage.jar && \
    curl -L "https://repo1.maven.org/maven2/io/delta/delta-streaming_${SCALA_VERSION}/${DELTA_LAKE_VERSION}/delta-streaming_${SCALA_VERSION}-${DELTA_LAKE_VERSION}.jar" --output /opt/spark/jars/delta-streaming.jar && \
    curl -L "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_LAKE_VERSION}/delta-storage-${DELTA_LAKE_VERSION}.jar" --output /opt/spark/jars/delta-storage.jar && \
    # AWS S3 Jars
    curl -L "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" --output /opt/spark/jars/hadoop-aws.jar && \
    curl -L "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" --output /opt/spark/jars/aws-java-sdk-bundle.jar
