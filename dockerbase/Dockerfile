FROM ubuntu:20.04

# Install java 11, sbt and scala
RUN apt-get update && apt-get install -y openjdk-11-jdk curl gnupg2 &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list &&\
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add &&\
apt-get update && apt-get install -y sbt=1.8.0 openssh-server

# Download and install spark with hadoop
RUN curl -O https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz &&\
tar xvf spark-3.0.0-bin-hadoop3.2.tgz && mv spark-3.0.0-bin-hadoop3.2/ /opt/spark && rm spark-3.0.0-bin-hadoop3.2.tgz

# Create the required directories
RUN mkdir /app
RUN mkdir /app/input
VOLUME /app/input
RUN mkdir /app/output
VOLUME /app/output
WORKDIR /app

# Copy the project inside the dockerfile
COPY dockerbase/build.sbt /app/build.sbt
COPY src /app/src
COPY project /app/project

# Build the project into a single jar
RUN sbt clean assembly
RUN mv target/scala-2.12/app-assembly-0.1.jar preprocess.jar

# Parameters depending on the database used. These are the default values should adjust appropriately
# before building the image.
ENV cassandra_host=localhost
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
ENV s3endPointLoc=http://localhost:9000
ENV s3secretKeyAws=minioadmin


ENTRYPOINT ["/opt/spark/bin/spark-submit","--master","local[*]","preprocess.jar"]
CMD ["-t 200 "]
