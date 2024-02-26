"""
Created on Tue Jan 12 11:31:21 2021

@author: mavroudo
"""
from datetime import datetime, timedelta
import random
import string


def generate_events(number_of_events, prefix=""):
    defined_prefix = ""
    if prefix:
        defined_prefix = prefix
    else:
        if number_of_events > 26:
            defined_prefix = "GE"
    if not defined_prefix:
        return list(string.ascii_uppercase)[:number_of_events]
    else:
        event_list = []
        for i in range(1, number_of_events + 1):
            event_list.append(defined_prefix + str(i).zfill(len(str(number_of_events))))
        return event_list


def generate_sequences(events_length, number_of_traces, min_trace_length, max_trace_length):
    events = generate_events(events_length)
    sequences = []
    for _ in range(0, number_of_traces):
        trace = []
        for _ in range(0, random.randint(min_trace_length, max_trace_length)):
            trace.append(random.choice(events))
        sequences.append(trace)

    return sequences


def generate_timestamps(initial_date: str, number: int):
    frmt = "%Y-%m-%d %H:%M:%S"
    date = datetime.strptime(initial_date, frmt)  # initialize it
    for _ in range(number):
        date = date + random.random() * timedelta(hours=1)
        yield date.strftime(frmt)


def create_file(filename, initial_date, events_length, number_of_traces, min_trace_length, max_trace_length,  s=",", s_index="::", s_time="/delab/"):
    sequences = generate_sequences(events_length, number_of_traces, min_trace_length, max_trace_length)
    lines = []
    for index, sequence in enumerate(sequences):
        times = generate_timestamps(initial_date, len(sequence))
        string = str(index) + s_index
        for event in sequence:
            string += event + s_time + next(times) + s
        lines.append(string[:-1])
    with open("logs/" + filename + ".withTimestamp", "w") as f:
        for line in lines:
            f.write(line + "\n")


datasets = [
    ["1",
"2",
"3",
"4",
"5",
"6",
"7",
"8",
"9",
"10",
"11",
"12",],
    [100,
100,
100,
100,
100,
100,
100,
100,
10,
100,
1000,
10000,],
    [5,
10,
100,
1000,
100,
100,
100,
100,
100,
100,
100,
100,],
    [80,
80,
80,
80,
5,
80,
800,
8000,
80,
80,
80,
80,],
    [120,
120,
120,
120,
15,
120,
1200,
12000,
120,
120,
120,
120,]
]

for i in range(0, len(datasets[0])):
    create_file(datasets[0][i], "2023-02-24 15:10:10", datasets[1][i], datasets[2][i], datasets[3][i], datasets[4][i])
