# Use the official jupyter/pyspark-notebook image as a base
FROM jupyter/pyspark-notebook:spark-3.4.0

# We switch to the 'root' user to install system-wide dependencies.
USER root

# Install Python dependencies from your project's requirements.txt
# We copy it into a temporary location to use it.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# --- Download Spark Connector Jars ---
# These versions should match your pyspark_build/Dockerfile to ensure consistency.
ENV DELTA_LAKE_VERSION=2.4.0
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV SCALA_VERSION=2.12

# Download Delta and AWS JARs into Spark's jars directory
RUN curl -L "https://repo1.maven.org/maven2/io/delta/delta-core_${SCALA_VERSION}/${DELTA_LAKE_VERSION}/delta-core_${SCALA_VERSION}-${DELTA_LAKE_VERSION}.jar" --output /opt/spark/jars/delta-core.jar && \
    curl -L "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_LAKE_VERSION}/delta-storage-${DELTA_LAKE_VERSION}.jar" --output /opt/spark/jars/delta-storage.jar && \
    curl -L "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" --output /opt/spark/jars/hadoop-aws.jar && \
    curl -L "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" --output /opt/spark/jars/aws-java-sdk-bundle.jar

# After installations, switch back to the default 'jovyan' user
# to maintain the security and operational context of the base image.
USER ${NB_UID}

# The working directory is already set to /home/jovyan/work in the base image.