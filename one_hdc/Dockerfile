FROM eclipse-temurin:11-jdk

ENV HADOOP_VERSION 3.3.6
ENV SPARK_VERSION 3.5.2

ADD tars/hadoop-${HADOOP_VERSION}.tar.gz  /opt
ADD tars/spark-${SPARK_VERSION}-bin-hadoop3.tgz  /opt

RUN ln -s /opt/hadoop-${HADOOP_VERSION}/etc/hadoop /etc/hadoop \
    && mkdir /opt/hadoop-${HADOOP_VERSION}/logs \
    && mkdir /hadoop-data


RUN mkdir -p /opt/spark \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /tmp/spark \
    && cp -r /tmp/spark/* /opt/spark 


USER root


RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      openjdk-11-jdk \
      ssh \
      rsync \
      net-tools \
      curl \
      netcat-openbsd \
      gnupg \
      procps \
      libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*
      
ENV JAVA_HOME=/opt/java/openjdk


ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV USER=root
ENV PATH $HADOOP_HOME/bin/:$PATH


ENV SPARK_HOME=/opt/spark
ENV PATH=$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH


ADD one_hdc/entrypoint.sh /entrypoint.sh

RUN chmod a+x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
