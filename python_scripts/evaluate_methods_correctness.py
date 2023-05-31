#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 31 11:15:28 2023

@author: mavroudo
"""
import random,string
from datetime import datetime,timedelta

# =============================================================================
# Original Method
# =============================================================================
def extract_single_events(trace):
    single={}
    for event in trace:
        if event["name"] not in single:
            single[event["name"]]=[]
        single[event["name"]].append(event["timestamp"])
    return single      

def create_combinations(single):
    alist=list(single)
    return [(i,j) for i in alist for j in alist]

def create_pairs(c1,c2,ts_a:list,ts_b:list):
    pairs=[]
    ts_inner=ts_b.copy()
    prev=None
    for ea in ts_a:
        if prev is not None and ea<prev:
            continue
        to_be_removed=[]
        for eb in ts_inner:
            if ea >= eb:
                to_be_removed.append(eb)
            else:
                pairs.append([c1,c2,ea,eb])
                prev=eb
                to_be_removed.append(eb)
                break
        ts_inner=[i for i in ts_inner if i not in to_be_removed]
    return pairs

def original_method(trace):
    # extract single events
    single = extract_single_events(trace)
    # create 2 combinations
    combinations = create_combinations(single)
    # create_pairs 
    pairs=[]
    for c in combinations:
        p=create_pairs(c[0],c[1],single[c[0]],single[c[1]])
        if p!=[]:
            for pair in p:
                pairs.append(pair)
    return pairs
        
# =============================================================================
#       Methods to generate custom logfile  
# =============================================================================
def generate_event_list(event_names, num_events):
    events = []
    timestamp = datetime.now()
    for _ in range(num_events):
        event_name = random.choice(event_names)
        event = {
            "name": event_name,
            "timestamp": timestamp
        }
        events.append(event)
        timestamp += timedelta(seconds=1)
    return events
        
def generate_traces(minimum,maximum,number,event_names):
    traces=[]
    for _ in range(number):
        n = random.randint(minimum,maximum)
        traces.append(generate_event_list(event_names,n))
    return traces
        
def generate_event_name_list(num_names):
    event_names = []
    for _ in range(num_names):
        event_name = ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 10)))
        event_names.append(event_name)
    return event_names



# =============================================================================
# Incremental Method
# =============================================================================
def incremental_method(trace):
    pairs=[]
    for i in range(len(trace)):
        event=trace[i]
        prev_trace=trace[:i]
        la = detect_last_appearence(event,prev_trace)
        used=[]
        for e in prev_trace[la:]:
            if e["name"] not in used:
                pairs.append([e["name"],event["name"],e["timestamp"],event["timestamp"]])
                used.append(e["name"])
        
    return pairs

def detect_last_appearence(event,prev_trace):
    for index,e in reversed(list(enumerate(prev_trace))):
        if e["name"]==event["name"]:
            return index
    return 0

# =============================================================================
# Evaluation methods
# =============================================================================
def find_differences(list1, list2):
    flattened_list1 = [tuple(item) if isinstance(item, list) else item for item in list1]
    flattened_list2 = [tuple(item) if isinstance(item, list) else item for item in list2]
    set1 = set(flattened_list1)
    set2 = set(flattened_list2)
    differences = set1.symmetric_difference(set2)
    return differences

def find_diff_from_First(list1, list2):
    flattened_list1 = [tuple(item) if isinstance(item, list) else item for item in list1]
    flattened_list2 = [tuple(item) if isinstance(item, list) else item for item in list2]
    set1 = set(flattened_list1)
    set2 = set(flattened_list2)
    differences = [i for i in set1 if i not in set2]
    return differences

def get_specific_pairs(eventa,eventb,results,trace_id):
    pairs=[]
    for pair in results[trace_id]:
        if pair[0]==eventa and pair[1]==eventb:
            pairs.append(pair)
    return pairs
    
# =============================================================================
# Testing section
# =============================================================================
# create a random logfile
event_names = generate_event_name_list(30)
traces = generate_traces(10,100,100,event_names)

# original method pairs
pairs_original = {index:original_method(t) for index,t in enumerate(traces)}

# incremental method
pairs_incremental = {index:incremental_method(t) for index,t in enumerate(traces)}

for i in pairs_original:
    diff = find_differences(pairs_incremental[i],pairs_original[i])
    if len(diff) >0:
        print(i)
