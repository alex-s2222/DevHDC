FROM apache/spark:3.5.2

USER root

ENV SPARK_VERSION=3.5.2
ENV ICEBERG_VERSION=1.4.3
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

WORKDIR /opt/spark

########################################
# 1. Iceberg Spark runtime
########################################
RUN curl -L -o jars/iceberg-spark-runtime.jar \
https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar

########################################
# 2. Hadoop AWS (S3A support)
########################################
RUN curl -L -o jars/hadoop-aws.jar \
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar

########################################
# 3. AWS SDK bundle (required by S3A)
########################################
RUN curl -L -o jars/aws-java-sdk-bundle.jar \
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar


ENV PATH="/opt/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

########################################
# 4. Ensure permissions
########################################
RUN chmod -R 755 /opt/spark/jars

USER spark