FROM openjdk:11 AS builder
RUN apt-get update && apt-get install -y gnupg2 curl &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list &&\
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add &&\
apt-get update && apt-get install -y sbt=1.10.0




RUN mkdir /app
COPY dockerbase/build2.sbt /app/build2.sbt
COPY src /app/src
COPY project /app/project

WORKDIR /app
RUN sbt clean assembly
RUN mv target/scala-2.12/app-assembly-0.1.jar preprocess.jar


FROM openjdk:11 AS execution
RUN apt-get update && apt-get install -y gnupg2 curl procps python3 python3-pip



# Set up Spark
RUN mkdir /app
WORKDIR /app

# Install necessary Python packages (if required)
COPY python_scripts/requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY python_scripts/stream_withTimestamp.py /app/scripts/stream_withTimestamp.py
COPY experiments/input/helpdesk.withTimestamp /app/experiments/helpdesk.withTimestamp
COPY experiments/input/bpi2018.withTimestamp /app/experiments/bpi2018.withTimestamp

RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
    tar xvf spark-3.5.4-bin-hadoop3.tgz && mv spark-3.5.4-bin-hadoop3/ /opt/spark && rm spark-3.5.4-bin-hadoop3.tgz

# Download Delta Lake JARs
RUN mkdir /jars
RUN curl -O https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -O https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.jar && \
    mv delta-spark_2.12-3.2.0.jar /jars/ && \
    mv spark-sql-kafka-0-10_2.12-3.5.4.jar /jars/

# Copy preprocess.jar from the builder stage
COPY --from=builder /app/preprocess.jar preprocess.jar

# Spark environment
ENV SPARK_CLASSPATH=/jars/*:/app/preprocess.jar

# Configuration variables
ENV kafkaBroker=siesta-kafka:9092
ENV kafkaTopic=test
ENV POSTGRES_ENDPOINT=siesta-postgres:5432/metrics
ENV POSTGRES_PASSWORD=admin
ENV POSTGRES_USERNAME=admin
ENV s3accessKeyAws=minioadmin
ENV s3ConnectionTimeout=600000
ENV s3endPointLoc=minio:9000
ENV s3secretKeyAws=minioadmin

# Spark-submit entrypoint
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--jars", "/jars/*", "--class", "auth.datalab.siesta.siesta_main",  "preprocess.jar"]
CMD ["--driver-memory", "10g"]
#CMD ["--logname", "test", "--delete_prev", "--system", "streaming"]
