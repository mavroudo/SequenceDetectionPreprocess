#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 10:54:20 2023

@author: mavroudo
"""

import sys,random,time,requests
import socket,json
from kafka import KafkaProducer

def read_file(file):
    data ={}
    with open(file,"r") as f:
        for line in f:
            trace = int(line.split("::")[0])
            events = line.split("::")[1].split(",")        
            data[trace]=[]
            for event in events:
                o={}
                o["trace"]=trace
                o["event_type"]=event.split("/delab/")[0].strip()
                o["timestamp"]=event.split("/delab/")[1].strip()
                data[trace].append(o)   
    return data

def reorder_events(data,events_per_second):
    interval = 1/events_per_second
    counters={i:0 for i in data.keys()}
    non_empty =[i for i in data.keys()]
    all_events = sum([len(data[x]) for x in data])
    for i in range(all_events):
        trace = random.choice(non_empty)
        yield data[trace][counters[trace]]
        time.sleep(interval)
        counters[trace]+=1
        if counters[trace]==len(data[trace]):
            non_empty.remove(trace)
                
    

if __name__ == "__main__":
    #file = sys.argv[1]
    #eventsPerSecond = int(sys.argv[2])
# =============================================================================
#     Demo params for now
# =============================================================================
    file="experiments/input/synthetic_0.withTimestamp"
    eventsPerSecond=200
    print("Streaming {} file, with {} events per second".format(file,eventsPerSecond))
    producer = KafkaProducer(bootstrap_servers='localhost:29092',value_serializer=lambda v: json.dumps(v).encode('utf-8'),key_serializer=lambda k: str(k).encode('utf-8'))
    data=read_file(file)
    print("Number of events in this logfile: {}".format(len(data)))
    for event in reorder_events(data,eventsPerSecond):
        print("sending {}".format(str(event)))
        producer.send('test',key=event["trace"], value=event)
    
    
    