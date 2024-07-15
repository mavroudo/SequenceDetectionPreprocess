FROM mavroudo/spark-base

ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8080
ENV SPARK_MASTER_LOG=/spark/logs

RUN wget https://downloads.lightbend.com/scala/2.12.12/scala-2.12.12.deb
RUN dpkg -i scala-2.12.12.deb

EXPOSE 8080
EXPOSE 7077
ENTRYPOINT $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master
