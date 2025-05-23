FROM ubuntu:20.04
RUN apt-get update && apt-get install -y gnupg2 curl wget openjdk-11-jdk

RUN wget https://downloads.lightbend.com/scala/2.12.17/scala-2.12.17.deb
RUN dpkg -i scala-2.12.17.deb
ENV SPARK_HOME=/opt/spark


RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
    tar xvf spark-3.5.4-bin-hadoop3.tgz && mv spark-3.5.4-bin-hadoop3/ $SPARK_HOME && rm spark-3.5.4-bin-hadoop3.tgz


ENV s3accessKeyAws=minioadmin
ENV s3ConnectionTimeout=600000
ENV s3endPointLoc=http://minio:9000
ENV s3secretKeyAws=minioadmin