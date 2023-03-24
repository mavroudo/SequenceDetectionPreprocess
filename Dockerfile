FROM ubuntu:20.04

RUN mkdir /app
COPY build.docker.sbt /app/build.sbt
COPY src /app/src
COPY project /app/project

ENV TZ=Europe/Athens
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
#install the sbt and java 8 needed for sbt assembly
RUN apt-get update && apt-get install -y curl gnupg2 &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list &&\
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add &&\
apt-get update &&\
apt-get install -y sbt=1.3.13 openjdk-8-jdk
#build the application and save it under the name app/preprocess.jar
WORKDIR /app
RUN sbt clean assembly
RUN mv target/scala-2.11/app-assembly-0.1.jar preprocess.jar
#install spark
RUN curl -O https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz &&\
tar xvf spark-2.4.4-bin-hadoop2.7.tgz && mv spark-2.4.4-bin-hadoop2.7/ /opt/spark && rm spark-2.4.4-bin-hadoop2.7.tgz

#create directories to mount
RUN mkdir /app/input
VOLUME /app/input
RUN mkdir /app/output
VOLUME /app/output

ENV cassandra_host=
ENV cassandra_port=
ENV cassandra_user=
ENV cassandra_pass=
ENV cassandra_keyspace_name=sequenceDetection
ENV cassandra_keyspace_name_set=setcontainment
ENV cassandra_replication_class=SimpleStrategy
ENV cassandra_replication_rack=replication_factor
ENV cassandra_replication_factor=3
ENV cassandra_write_consistency_level=ONE
ENV cassandra_gc_grace_seconds=864000


ENTRYPOINT ["/opt/spark/bin/spark-submit","--master","local[*]","--conf","spark.cassandra.output.consistency.level=ONE","preprocess.jar"]
CMD ["-t 200 "]
