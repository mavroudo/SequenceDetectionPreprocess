from datetime import datetime
import sys
import pm4py

'''
    This script reads a file with sequences of activity instances and their end-time timestamps, and creates a dict with the following schema:
        *   key = trace index: int
        *   value = array<struct<event_type: str, start_timestamp: datetime, end_timestamp: datetime, resourse: str>>

    We define concurency of two activity instances (a.k.a. tasks) as described in the paper "Split Miner:
    Discovering Accurate and Simple Business Process Models from Event Logs" (Augusto et al. 2017, https://kodu.ut.ee/~dumas/pubs/icdm2017-split-miner.pdf).
    Tasks A and B are concurrent iff:
        *   there are 2 traces in log L such that in one trace A is directly followed by B, and in the other trace B is directly followed by A.
        *   there is no trace in log L such that A is directly followed by B and B is directly followed by A.
        *   the ratio (| |A->B| - |B->A| |) / (|A->B| + |B->A|) is less than 1.

    The start time of an activity instance (task) is determined in two phases as described in the paper "Repairing Activity Start Times
    to Improve Business Process Simulation" (Chapela-Campa, Dumas 2017, https://arxiv.org/pdf/2208.12224.pdf):
        1.  Calculate the enablement time of the tasks based on the concurrency of the tasks.
            The enablement time of a task is defined as:
            *   the pre-defined trace start time, if the task is first in the trace.
            *   the end time of the last previous task in the trace, if the task is not first in the trace and that last previous task
                is not concurrent with it.
        2.  Fix the enablement time of the tasks based on the resource availability.
            The start time of the task is defined as the maximum of: the enablement time of the task, and the end time of the last task
            of the same resource which is previous to its end time.
'''


class Task:
    def __init__(self, event_type, end_timestamp, start_timestamp=None, resource=None):
        self.event_type: str = event_type
        self.end_timestamp: datetime = end_timestamp #datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')
        self.start_timestamp: datetime = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S') if start_timestamp else None
        self.resource: str = resource

    def __str__(self) -> str:
        return f"Task({self.event_type}, {self.start_timestamp if self.start_timestamp else '?'}, {self.end_timestamp}, {self.resource if self.resource else '?'})"


def printTasks(traces, duration=False):
    for trace_index, tasks in traces.items():
        print(f"Trace {trace_index}:")
        for task in tasks:
            print(f"\t{task}", end=" " if duration else "\n")
            if duration:
                print(f"-> {divmod((task.end_timestamp - task.start_timestamp).total_seconds(), 60)[0]} min")

def ingest_data_txt(file_name, separator, delimiter):
    # Set of types of activity instances, e.g. "A", "B", "C"
    types = set()
    # Pairs of activity instances, e.g. "A->B", "B->C"
    pairs = dict()
    # Dict of resources and their tasks
    resources = dict()
    # Dictionary of traces and their tasks
    traces = {}

    with open(file_name, 'r') as reader:
        for line in reader:
            parts = line.strip().split("::")

            trace_index = int(parts[0])
            traces[trace_index] = []

            tasks = parts[1].split(separator)
            for i, task in enumerate(tasks):
                type = task.split(delimiter)[0]
                types.add(type)
                end = task.split(delimiter)[1]
                resource = task.split(delimiter)[2]

                task = Task(event_type=type, end_timestamp=end, resource=resource)

                if resource not in resources.keys():
                    resources[resource] = [task]
                else:
                    resources[resource].append(task)

                traces[trace_index].append(task)

                if i < len(tasks) - 1:
                    if type + tasks[i+1].split(delimiter)[0] in pairs.keys():
                        pairs[type + tasks[i+1].split(delimiter)[0]] += 1
                    else:
                        pairs[type + tasks[i+1].split(delimiter)[0]] = 1
                        pairs[tasks[i+1].split(delimiter)[0] + type] = 0

    return traces, pairs, types, resources


def ingest_data(file_name, separator=None, delimiter=None):
    if (not file_name.endswith(".xes")):
        return ingest_data_txt(file_name, separator, delimiter)

    # Set of types of activity instances, e.g. "A", "B", "C"
    types = set()
    # Pairs of activity instances, e.g. "A->B", "B->C"
    pairs = dict()
    # Dict of resources and their tasks
    resources = dict()
    # Dictionary of traces and their tasks
    traces = dict()

    # Read the XES file
    log = pm4py.read_xes(file_name)

    for index, event in log.iterrows():
        type = event['concept:name']
        end = event['time:timestamp']
        resource = event['org:resource']

        types.add(type)

        task = Task(event_type=type, end_timestamp=end, resource=resource)

        if not event['case:Rfp_id'] in traces.keys():
            last_event = None
            traces[event['case:Rfp_id']] = [task]
        else:
            last_event = traces[event['case:Rfp_id']][-1].event_type
            traces[event['case:Rfp_id']].append(task)

        if not resource in resources.keys():
            resources[resource] = [task]
        else:
            resources[resource].append(task)

        if last_event is not None:
            if last_event + type in pairs.keys():
                pairs[last_event + '~~' + type] += 1
            else:
                pairs[last_event + '~~' + type] = 1
                pairs[type + '~~' + last_event] = 0

    return traces, pairs, types, resources

def find_concurrency(traces, pairs, types) -> set:
    non_concurrents = set()
    candidates = set()

    # Find all candidate concurrent activity pairs
    # O(n^2) but since n is small, it's ok
    for i in types:
        for j in types:
            if i != j:
                candidates.add(i + '~~' + j)

    # Exonerate certain non-concurrent pairs based on the first condition
    # O(n) but since n is small, it's ok
    for candidate in candidates:
        if candidate not in pairs.keys():
            non_concurrents.add(candidate)
            non_concurrents.add(candidate.split('~~')[1] + '~~' + candidate.split('~~')[0])
        else:
            if pairs[candidate] == 0:
                non_concurrents.add(candidate)
                non_concurrents.add(candidate.split('~~')[1] + '~~' + candidate.split('~~')[0])

    # Exonerate certain non-concurrent pairs based on the second condition
    # O(m*n) but since n is small, it's ok
    for tasks in traces.values():
        for i in range(1, len(tasks) - 1):
            if tasks[i - 1].event_type == tasks[i + 1].event_type:
                non_concurrents.add(tasks[i-1].event_type + tasks[i].event_type)
                non_concurrents.add(tasks[i].event_type + tasks[i-1].event_type)

    # Clear the candidates set from the non-concurrent pairs
    for nc in non_concurrents:
        if nc in candidates:
            candidates.remove(nc)

    # Exonerate certain non-concurrent pairs based on the third condition
    for candidate in candidates:
        candidate_reversed = candidate.split('~~')[1] + '~~' + candidate.split('~~')[0]
        if abs(pairs[candidate] - pairs[candidate_reversed]) / (pairs[candidate] + pairs[candidate[candidate_reversed]]) >= 1:
            non_concurrents.add(candidate)
            non_concurrents.add(candidate[candidate_reversed])

    # Final cleaning the candidates set from the non-concurrent pairs
    for nc in non_concurrents:
        if nc in candidates:
            candidates.remove(nc)

    return candidates


def determine_enablement_times(traces, concurrents):
    for trace_index, tasks in traces.items():
        for i, task in enumerate(tasks):
            # Start time policy for the first task of a trace:
            #   *   If the first task's end time is not at the exact hour, then the start time is the exact hour.
            #   *   If the first task's end time is at the exact hour, then the start time is the exact hour minus one hour.
            if i == 0:
                if task.end_timestamp.minute != 0 or task.end_timestamp.second != 0:
                    task.start_timestamp = task.end_timestamp.replace(minute=0, second=0, microsecond=0)
                else:
                    if task.end_timestamp.hour == 0:
                        task.start_timestamp = task.end_timestamp.replace(hour=23, minute=0, second=0, microsecond=0)
                    else:
                        task.start_timestamp = task.end_timestamp.replace(hour=task.end_timestamp.hour-1, minute=0)
            else:
                if tasks[i-1].event_type + task.event_type in concurrents:
                    task.start_timestamp = tasks[i-1].start_timestamp
                else:
                    task.start_timestamp = tasks[i-1].end_timestamp


def fix_start_times(traces, resources):
    # Fix the start time of the tasks based on the resource availability.
    # The start time of the task is defined as the maximum of: the enablement time of the task,
    # and the end time of the last task of the same resource which is previous to its end time.
    for trace_index, tasks in traces.items():
        for task in tasks:
            available_times = [t.end_timestamp for t in resources[task.resource] if t.end_timestamp < task.end_timestamp]
            if available_times and task.start_timestamp < max(available_times):
                task.start_timestamp = max(available_times)


if __name__ == '__main__':
    """ Parameters """
    # The path of the log file
    file_name = sys.argv[1]
    # The separator between tasks of the same trace
    separator = ","
    # The delimiter between the task's attributes
    delimiter = "/delab/"


    """ Pipeline """
    # Dictionary of traces and their tasks
    traces , pairs , types, resources = ingest_data(file_name)

    # Set of concurrent pairs of tasks
    concurrents = find_concurrency(traces, pairs, types)

    # Find the start time of the tasks based on the concurrent activities and the resource availability
    # Update the traces by estimating the ebablement time of the tasks
    determine_enablement_times(traces, concurrents)
    # Fix the start time of the tasks based on the resource availability
    fix_start_times(traces, resources)

    # print(f"Pairs: {pairs}")
    # print(f"Types: {types}")
    print(f"Concurrents: {concurrents if concurrents else 'No concurrent pairs found'}")
    printTasks(traces, duration=True)