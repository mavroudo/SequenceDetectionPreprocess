#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 09:48:21 2020

@author: mavroudo
"""
import random
import string
import timeit

import matplotlib.pyplot as plt


class Event:
    def __init__(self, timestamp, event_name):
        self.timestamp = timestamp
        self.event_name = event_name

    def __str__(self):
        return str(self.event_name)


class Pair:
    def __init__(self, event_a, event_b):
        self.first_event = None
        self.event_a: str = event_a
        self.event_b: str = event_b
        self.count: int = 0
        self.pairs = []

    def same_event(self, timestamp):
        self.pairs.append(timestamp)
        self.first_event = timestamp
        self.count += 1

    def __str__(self):
        return str(self.pairs) + " " + str(self.first_event)

    def __hash__(self):
        return hash(self.event_a) ^ hash(self.event_b)


class hashSet:
    def __init__(self, distinct_events):
        self.distinct_events = distinct_events
        start = timeit.default_timer()
        self.index = {str(i) + '_' + str(j): Pair(str(i), str(j)) for i in distinct_events for j in distinct_events}
        # print("Preprocess for ", str(len(distinct_events)), " distinct events " + str(timeit.default_timer() - start))

    def new_event(self, event: Event):
        self._check_as_first(event)
        self._check_as_second(event)

    def _check_as_first(self, event: Event):
        pair: Pair = self.index[str(event.event_name) + '_' + str(event.event_name)]
        self._check_the_same(pair, event)
        for second_event in self.distinct_events:
            if second_event != event.event_name:
                pair: Pair = self.index[str(event.event_name) + '_' + str(second_event)]
                if pair.count % 2 == 0:
                    pair.first_event = event.timestamp
                    pair.pairs.append(event.timestamp)
                    pair.count += 1

    def _check_as_second(self, event: Event):
        for first_event in self.distinct_events:
            if first_event != event.event_name:
                pair: Pair = self.index[str(first_event) + '_' + str(event.event_name)]
                if pair.count % 2 == 1:
                    pair.pairs.append(event.timestamp)
                    pair.count += 1

    def _check_the_same(self, pair: Pair, event: Event):
        pair.first_event = event.timestamp
        pair.pairs.append(event.timestamp)
        pair.count += 1

    def get_answer(self):
        response = dict()
        for pair in self.index:
            pair_list = self.index[pair].pairs
            if len(pair_list) % 2 != 0:
                response[pair] = pair_list[:-1]
            else:
                response[pair] = pair_list
        return response


def createSequence(l: list, length: int):
    return random.choices(l, k=length)


def n_square_method(sequence: list):
    checked = set()
    index = dict()
    for timeA, eventA in enumerate(sequence):  # use index as time
        if eventA not in checked:
            loop = set()
            for indexing, eventB in enumerate(sequence[timeA + 1:]):
                timeB = timeA + indexing + 1
                identification = str(eventA) + '_' + str(eventB)
                if eventA == eventB:
                    if identification not in index:
                        index[identification] = [timeB, timeA]
                    else:
                        index[identification].insert(0, timeB)
                    for r in loop:
                        index[str(eventA) + '_' + str(r)].insert(0, timeB)
                    loop.clear()
                elif eventB not in loop:
                    if identification not in index:
                        index[identification] = [timeB, timeA]
                    else:
                        index[identification].insert(0, timeB)
                    loop.add(eventB)
            checked.add(eventA)
    for row in index:
        if len(index[row]) % 2 != 0:
            index[row] = index[row][1:]
        index[row].reverse()
    return index


def state_method(sequence: list):
    distinct_elements = list(set(sequence))  # in this sequence
    data = hashSet(distinct_elements)
    time_for_events = 0
    for index, event in enumerate(sequence):
        start = timeit.default_timer()
        data.new_event(Event(index, str(event)))
        time_for_events += timeit.default_timer() - start
    # print("Time per event " + str(time_for_events / len(sequence)))
    return data.get_answer()


def creatingPairs(indexingA, indexingB):  # O(n) to create pairs
    posA, posB = 0, 0
    prev = -1  # this will be the previous time, or index
    response = []
    while posA < len(indexingA) and posB < len(indexingB):
        if indexingA[posA] < indexingB[posB]:
            if indexingA[posA] > prev:
                response += [indexingA[posA], indexingB[posB]]
                prev = indexingB[posB]
                posA += 1
                posB += 1
            else:
                posA += 1
        else:
            posB += 1
    return response


def indexing_method(sequence: list):
    mapping = dict()
    for index, ev in enumerate(sequence):  # O(n) indexing elements
        if ev in mapping:
            mapping[str(ev)].append(index)
        else:
            mapping[str(ev)] = [index]
    response = dict()
    for eventA in mapping:  # O(l^2) takes place
        for eventB in mapping:
            response[str(eventA) + '_' + str(eventB)] = creatingPairs(mapping[eventA], mapping[eventB])
    return response


def plot_graph_compare(fileName):
    data = []
    with open(fileName, "r") as f:
        for index, line in enumerate(f):
            if index != 0:
                data.append(line.split(","))
    for index, i in enumerate(data):
        data[index] = [int(i[0]), float(i[1]), float(i[2]), float(i[3])]

    plt.plot([i[0] for i in data], [i[1] for i in data], label="Current method (n^2)")
    plt.plot([i[0] for i in data], [i[2] for i in data], label="Indexing method")
    plt.plot([i[0] for i in data], [i[3] for i in data], label="State method")
    plt.xlabel("Sequence length")
    plt.ylabel("Time (s)")
    plt.title("Time compare to n, l=26")
    plt.legend()
    plt.savefig("time_compare_methods_3.png")


def plot_graph_l(data):
    plt.figure(0)
    plt.plot([i[0] for i in data], [i[1] for i in data], label="Current method (n^2)")
    plt.plot([i[0] for i in data], [i[2] for i in data], label="Indexing method")
    plt.plot([i[0] for i in data], [i[3] for i in data], label="State method")
    plt.xlabel("l")
    plt.ylabel("Time (s)")
    plt.legend()
    plt.title("Time compare to l, n=100.000")
    plt.savefig("time_compare_l_3.png")


def event_generator(size=8, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


if __name__ == "__main__":
    chars = [i for i in "abcdefghijklmnopqrstuvwxyz"]
    times = []
    sequence = ['a', 'a', 'b', 'a', 'b', 'a']

    # this method compares the 2 methods with l=26 and n from 10-1.000.000
    fileName = "time_compare_methods_3.txt"
    for i in range(1, 7):
        print(i)
        sequence = createSequence(chars, 10 ** i)
        # sequence=['a','a','b','a','b','a']
        start_n = timeit.default_timer()
        results_n = n_square_method(sequence)
        stop_n = timeit.default_timer()
        start_i = timeit.default_timer()
        results_i = indexing_method(sequence)
        stop_i = timeit.default_timer()
        start_x = timeit.default_timer()
        results_x = state_method(sequence)
        stop_x = timeit.default_timer()
        times.append([10 ** i, stop_n - start_n, stop_i - start_i, stop_x - start_x])
    with open(fileName, "w") as f:
        f.write("Sequence length,Time n square,Time indexing,Time Data Structure\n")
        for experiment in times:
            f.write(str(experiment[0]) + "," + str(experiment[1]) + "," + str(experiment[2]) + "," + str(
                experiment[3]) + "\n")
    plot_graph_compare(fileName)
    # l=50
    # for n in [100,10000,100000,1000000,2000000]:
    #     print(l)
    #     chars = [event_generator() for i in range(l)]
    #     sequence = createSequence(chars, n)
    #     results_x=stage_method(sequence)
    #     resolut_i =indexing_method(sequence)
    #     print(resolut_i==results_x)
    # this method compares only index with different l
    n = 100000
    times = []
    for l in [5, 20, 100, 250, 500, 1000]:
        print(l)
        chars = [event_generator() for i in range(l)]
        sequence = createSequence(chars, n)
        start_i = timeit.default_timer()
        results_i = indexing_method(sequence)
        stop_i = timeit.default_timer()
        start_n = timeit.default_timer()
        results_n = n_square_method(sequence)
        stop_n = timeit.default_timer()
        start_x = timeit.default_timer()
        results_x = state_method(sequence)
        stop_x = timeit.default_timer()
        # print(l, stop_i - start_i, stop_n - start_n, stop_x-start_x)
        times.append([l, stop_n - start_n, stop_i - start_i, stop_x - start_x])
    with open("n_100000_diff_l_3.txt", "w") as f:
        for exp in times:
            f.write(str(exp[0]) + "," + str(exp[1]) + "," + str(exp[2]) + "," + str(exp[3]) + "\n")
    plot_graph_l(times)
