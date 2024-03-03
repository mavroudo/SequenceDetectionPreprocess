#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 10:22:03 2024

@author: mavroudo
"""

import pandas as pd
from analyze_streaming_experiments import plot_functions

def split_to_datasets(df:pd.DataFrame):
    splitted=[]
    current=[]
    for index,row in df.iterrows():
        if row['batch_id']==0:
            if current!=[]:
                splitted.append(pd.DataFrame(current))
                current=[]
        else:
            current.append(df.iloc[index].to_dict())
    if current!=[]:
                splitted.append(pd.DataFrame(current))
    return splitted

def events_from_log(logname):
    ev=[]
    traces=0
    with open(logname,'r') as f:     
        for line in f:
            traces+=1
            s=line.split("::")[1].split(",")
            for e in s:
                event = e.strip().split("/delab/")
                ev.append({"event_type":event[0],"timestamp":event[1]})
    return pd.DataFrame(ev),traces

def extract_stats(events:pd.DataFrame):
    un = len(events['event_type'].unique())
    return un, events.shape[0]

def load_dataset_stats():
    stats=[]
    for i in range(1,13):
        events,traces = events_from_log("logs/{}.withTimestamp".format(i))
        un,num_ev = extract_stats(events)
        stats.append({"traces":traces,"unique_events":un,"events":num_ev})
    return pd.DataFrame(stats)
        
def calculate_stats_from_execution(execution:pd.DataFrame):
    stats={}
    # num of events indexed = number of rows in Write SingleTable
    stats["events_num"] = sum(execution[execution["query_name"]=="Write SingleTable"]["num_input_rows"])
    # num of event pairs = number of rows in Write IndexTable
    stats["pairs_num"] = sum(execution[execution["query_name"]=="Write SequenceTable"]["num_input_rows"])
    # duration is calculated as: the time the first event finished - its duration
    # until the time the last event ended
    start_time = execution.iloc[0]["ts"]-execution.iloc[0]["batch_duration"]
    end_time = execution.iloc[-1]["ts"]
    stats["total_duration"]= end_time - start_time
    #batches number is the maximum number of batch_id
    stats["batch_num"] = max(execution["batch_id"])
    # total time spend in each operation (they are running in parallel)
    stats["batch_duration"] = execution.groupby('query_name')["batch_duration"].sum().to_dict()
    return stats





stats= load_dataset_stats()     
data = pd.read_csv('logs/data-1709410922764.csv')
splitted = split_to_datasets(data)
all_stats = []
for s in splitted:
    all_stats.append(calculate_stats_from_execution(s))
    


# First graph: average time of total execution (for all batches) among all executions
# This will reveal which process takes the most time
plot_functions.plot_query_times(all_stats)

# Second graph: total duratiion required to index based on the number of traces
# extract the values for the [500,1.000,10.000,100.000]
durations=[]
for i in [0,1,2,3]: #indexes of the above values
    durations.append(int(all_stats[i]["total_duration"]/1000))
plot_functions.plot_durations_per_num_traces(durations,[5,10,100,1000])

#Third graph: total duration required to index based on the length of the traces
durations=[]
for i in [4,5,6,7]: #indexes of the above values
    durations.append(int(all_stats[i]["total_duration"]/1000))
plot_functions.plot_durations_per_trace_length(durations,[10,100,1000,10000])

#Fourth graph: total duration required to index based on the unique events
durations=[]
for i in [8,9,10,11]: #indexes of the above values
    durations.append(int(all_stats[i]["total_duration"]/1000))
plot_functions.plot_durations_per_unique_events(durations,[10,100,1000,10000])


# Calculate throughput based on events and event-pairs from the largest dataset (index 7)
#num_events/total time in seconds
throuput_event = int(all_stats[7]["events_num"]/(all_stats[7]["total_duration"]/1000))
print(f"Event throughput: {throuput_event} events/second")
# average events per batch
event_per_batch = int(all_stats[7]["events_num"]/(all_stats[7]["batch_num"]))
print(f"Average events per batch: {event_per_batch}")
#num_pairs/total time in seconds
# the event pairs can be extracted from the 'update metadata from IndexTable' query

















