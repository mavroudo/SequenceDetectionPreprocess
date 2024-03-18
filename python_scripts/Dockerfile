FROM python:3.9-slim

WORKDIR /usr/src/app

COPY ./requirements.txt .
RUN pip install -r requirements.txt
COPY logs/ /usr/src/app/logs/


COPY ./stream_withTimestamp.py .




ENV KAFKA_ENDPOINT=kafka:9093
ENV KAFKA_TOPIC=test
# Run app.py when the container launches
ENTRYPOINT ["python", "./stream_withTimestamp.py"]