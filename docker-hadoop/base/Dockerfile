FROM eclipse-temurin:11-jdk


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

RUN ssh-keygen -A

ENV HADOOP_VERSION 3.3.6

ADD tars/hadoop-${HADOOP_VERSION}.tar.gz  /opt

RUN ln -s /opt/hadoop-${HADOOP_VERSION}/etc/hadoop /etc/hadoop

RUN mkdir /opt/hadoop-${HADOOP_VERSION}/logs

RUN mkdir /hadoop-data

ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV USER=root
ENV PATH $HADOOP_HOME/bin/:$PATH

ADD entrypoint.sh /entrypoint.sh

RUN chmod a+x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
