#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 10:54:20 2023

@author: mavroudo
"""

import sys,random,time,requests
import socket,json
from kafka import KafkaProducer
from tqdm import tqdm

def read_file(file):
    data = []
    with open(file,"r") as f:
        for line in f:
            trace = line.split("::")[0]
            events = line.split("::")[1].split(",")        
            for index,event in enumerate(events):
                o={}
                o["trace"]=trace
                o["event_type"]=event.split("/delab/")[0].strip()
                o["timestamp"]=event.split("/delab/")[1].strip()
                o["position"]=index
                data.append(o)   
    data.sort(key=lambda x: x['timestamp'])
    return data


if __name__ == "__main__":
    #file = sys.argv[1]
    #eventsPerSecond = int(sys.argv[2])
# =============================================================================
#     Demo params for now
# =============================================================================
    file="experiments/input/bpi_2017_0.withTimestamp"
    #eventsPerSecond=1000
    #print("Streaming {} file, with {} events per second".format(file,eventsPerSecond))
    producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'),key_serializer=lambda k: str(k).encode('utf-8'))
    data= read_file(file)
    print("Number of events in this logfile: {}".format(len(data)))
    for event in tqdm(data,desc="Events sent",total=len(data)):
        producer.send('test',key=event["trace"], value=event)
        #time.sleep(1/eventsPerSecond)
    
    
    
