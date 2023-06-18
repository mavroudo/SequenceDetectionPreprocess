FROM openjdk:11-slim-buster AS builder
RUN apt-get update && apt-get install -y gnupg2 curl &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list &&\
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add &&\
apt-get update && apt-get install -y sbt=1.8.0

RUN mkdir /app
COPY dockerbase/build.sbt /app/build.sbt
COPY src /app/src
COPY project /app/project

WORKDIR /app
RUN sbt clean assembly
RUN mv target/scala-2.12/app-assembly-0.1.jar preprocess.jar


FROM python:3.8.17-alpine3.17
RUN apk --no-cache add curl
# Install python dependencies

RUN curl -O https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz &&\
tar xvf spark-3.0.0-bin-hadoop3.2.tgz && mv spark-3.0.0-bin-hadoop3.2/ /opt/spark && rm spark-3.0.0-bin-hadoop3.2.tgz

#RUN apt-get install -y python3-pip python
COPY pythonAPI/requirements.txt /app/pythonAPI/
RUN pip install -r /app/pythonAPI/requirements.txt
WORKDIR /app/pythonAPI

#Move python api folder here
COPY pythonAPI/main.py /app/pythonAPI/
COPY pythonAPI/EnvironmentVariables.py /app/pythonAPI/
COPY pythonAPI/PreprocessItem.py /app/pythonAPI/
RUN mkdir uploadedfiles

# install bash
RUN apk add bash
#Copy jar from the builder
COPY --from=builder /app/preprocess.jar preprocess.jar

# import default variables or can be changed here
ENV cassandra_host=cassandra
ENV cassandra_port=9042
ENV cassandra_user=cassandra
ENV cassandra_pass=cassandra
ENV cassandra_keyspace_name=siesta
ENV cassandra_replication_class=SimpleStrategy
ENV cassandra_replication_rack=replication_factor
ENV cassandra_replication_factor=3
ENV cassandra_write_consistency_level=ONE
ENV cassandra_gc_grace_seconds=864000
ENV s3accessKeyAws=minioadmin
ENV s3ConnectionTimeout=600000
ENV s3endPointLoc=http://minio-contact:9000
ENV s3secretKeyAws=minioadmin

#start the api
CMD ["python3","-m","uvicorn","main:app","--host", "0.0.0.0"]
