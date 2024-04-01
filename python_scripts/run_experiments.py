import subprocess, os, threading
import time

from stream_withTimestamp import read_file, reorder_events
from tqdm import tqdm
from kafka import KafkaProducer
import json
import psycopg2
import csv

docker_cmd = 'docker run --network siesta-net -p 4040:4040 preprocess --system streaming --delete_prev --logname test --streaming_events {}'


def send_event_to_kafka(filename, data, events_per_second, total):
    #start by sleeping for two minutes in order to allow the docker image to run appropriately
    time.sleep(120)
    kafka_endpoint = os.getenv("KAFKA_ENDPOINT", "localhost:9092")
    print("Kafka entrypoint: {}".format(kafka_endpoint))
    kafka_topic = os.getenv("KAFKA_TOPIC", "test")
    print("Streaming {} file, with {} events per second".format(filename, events_per_second))
    producer = KafkaProducer(bootstrap_servers=kafka_endpoint, value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                             key_serializer=lambda k: str(k).encode('utf-8'))
    progress_bar = tqdm(total=size,unit="events")
    for event in reorder_events(data, events_per_second):
        progress_bar.update(1)
        producer.send(kafka_topic, key=event["trace"], value=event)
    progress_bar.close()
    
def load_metrics_from_postgres(logfile):
    conn = psycopg2.connect(
        dbname="metrics",
        user="admin",
        password="admin",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    query = "SELECT * FROM logs;"
    cur.execute(query)
    rows = cur.fetchall()


    with open('results.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        if csvfile.tell() == 0:
            csv_writer.writerow([desc[0] for desc in cur.description] + ['logfile'])
        for row in rows:
            csv_writer.writerow(list(row) + [logfile])

    delete_query = """
        DELETE FROM logs
    """
    cur.execute(delete_query)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    events_per_second=200
    for logfile in os.listdir('logs'):
        if '.withTimestamp' in logfile:
            print(logfile)
            data = read_file('logs/{}'.format(logfile))
            size = sum([len(data[j]) for j in data])
            t1 =threading.Thread(target=send_event_to_kafka, args=(logfile, data,events_per_second, size))
            t1.start()
            subprocess.run(docker_cmd.format(size), shell=True)
            load_metrics_from_postgres(logfile=logfile)
            t1.join()
            break
