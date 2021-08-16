FROM ubuntu:20.04

RUN mkdir /app
COPY build.docker.sbt /app/build.sbt
COPY src /app/src
COPY project /app/project
COPY .environment /app/.env


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
#create the execution file
#RUN touch script.sh && echo "#!/bin/bash" > script.sh &&\
#echo "echo hello world" >> script.sh && chmod +x script.sh

#install python3, pip, requirements from experiments file
COPY experiments/requirements.txt r.txt
RUN apt install -y python3 python3-pip &&\
pip3 install -r r.txt
COPY experiments/execution.py execution.py
RUN chmod +x execution.py

#create directories to mount
RUN mkdir /app/input
VOLUME /app/input
RUN mkdir /app/output
VOLUME /app/output


ENTRYPOINT ["python3","/app/execution.py"]
CMD ["normal"," input/log_100_113.xes","indexing"]