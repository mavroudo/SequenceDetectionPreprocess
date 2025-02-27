FROM openjdk:11 AS builder
RUN apt-get update && apt-get install -y gnupg2 curl &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list &&\
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add &&\
apt-get update && apt-get install -y sbt=1.10.0




RUN mkdir /app
COPY dockerbase/build.sbt /app/build.sbt
COPY src /app/src
COPY project /app/project

WORKDIR /app
RUN sbt clean assembly
RUN mv target/scala-2.12/app-assembly-0.1.jar preprocess.jar


FROM ubuntu:20.04 AS execution
RUN apt-get update && apt-get install -y gnupg2 curl software-properties-common

#RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install -y python3.8 python3-pip openjdk-11-jdk
# Install python dependencies

RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
    tar xvf spark-3.5.4-bin-hadoop3.tgz && mv spark-3.5.4-bin-hadoop3/ /opt/spark && rm spark-3.5.4-bin-hadoop3.tgz

# Download Delta Lake and Kafka JARs
RUN mkdir /jars
RUN curl -O https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -O https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.jar && \
    mv delta-spark_2.12-3.2.0.jar /jars/ && \
    mv spark-sql-kafka-0-10_2.12-3.5.4.jar /jars/


#RUN apt-get install -y python3-pip python
COPY pythonAPI/requirements.txt /app/pythonAPI/
RUN pip install -r /app/pythonAPI/requirements.txt
WORKDIR /app/pythonAPI

#Move python api folder here
COPY pythonAPI/main.py /app/pythonAPI/
COPY pythonAPI/EnvironmentVariables.py /app/pythonAPI/
COPY pythonAPI/PreprocessItem.py /app/pythonAPI/
COPY pythonAPI/PreprocessTask.py /app/pythonAPI/
COPY pythonAPI/dbSQL /app/pythonAPI/dbSQL
RUN mkdir uploadedfiles

#Copy jar from the builder
COPY --from=builder /app/preprocess.jar /app/preprocess.jar

# import default variables or can be changed here
ENV kafkaBroker=siesta-kafka:9092
ENV kafkaTopic=test
ENV POSTGRES_ENDPOINT=siesta-postgres:5432/metrics
ENV POSTGRES_PASSWORD=admin
ENV POSTGRES_USERNAME=admin
ENV s3accessKeyAws=minioadmin
ENV s3ConnectionTimeout=600000
ENV s3endPointLoc=minio:9000
ENV s3secretKeyAws=minioadmin
#start the api
CMD ["python3","-m","uvicorn","main:app","--host", "0.0.0.0","--port","8000"]