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

#ENTRYPOINT ["tail", "-f", "/dev/null"]

FROM openjdk:11-slim-buster AS execution
RUN apt-get update && apt-get install -y gnupg2 curl
RUN mkdir /app
WORKDIR /app
RUN curl -O https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz &&\
tar xvf spark-3.0.0-bin-hadoop3.2.tgz && mv spark-3.0.0-bin-hadoop3.2/ /opt/spark && rm spark-3.0.0-bin-hadoop3.2.tgz
COPY --from=builder /app/preprocess.jar preprocess.jar

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
ENV s3endPointLoc=http://minio:9000
ENV s3secretKeyAws=minioadmin
ENV POSTGRES_ENDPOINT=postgres:5432/metrics
ENV POSTGRES_USERNAME=admin
ENV POSTGRES_PASSWORD=admin


ENTRYPOINT ["/opt/spark/bin/spark-submit","--master","local[12]","--conf","spark.driver.memory","15g","preprocess.jar"]
CMD ["--logname","test","--delete_prev"]