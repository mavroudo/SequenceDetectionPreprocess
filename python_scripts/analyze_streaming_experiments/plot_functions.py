import matplotlib.pyplot as plt

def plot_query_times(all_stats):
    total_times={}
    for s in all_stats:
        for q in s["batch_duration"]:
            if q not in total_times:
                total_times[q]=0
            total_times[q]+=s["batch_duration"][q]
    for q in total_times:
        # get the average time in seconds
        total_times[q]=int(total_times[q]/(len(all_stats)*1000))
    # Creating the bar plot
    plt.figure(figsize=(10, 6))
    plt.bar(total_times.keys(), total_times.values(), color='skyblue')
    plt.title('Average time of execution for the various queries across all experiments')
    plt.xlabel('Queries')
    plt.ylabel('Time (s)')
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()  # Adjust the layout to not cut off labels
    plt.savefig('plots/init_query_times.eps',format='eps')
    plt.show()
    
def plot_durations_per_num_traces(durations:list,traces:list):
    plt.figure(figsize=(10, 6))
    bars = plt.bar([str(x) for x in traces], durations, color='lightseagreen')

    # Add the value of each bar on top of the bar
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 10, yval, ha='center', va='bottom')

    plt.title('Total indexing duration based on number of traces')
    plt.xlabel('#traces')
    plt.ylabel('Indexing time (s)')
    plt.tight_layout()  # Adjust the layout to not cut off labels
    plt.savefig('plots/init_duration_per_traces.eps',format='eps')
    
def plot_durations_per_trace_length(durations:list,trace_length:list):
    plt.figure(figsize=(10, 6))
    bars = plt.bar([str(x) for x in trace_length], durations, color='lightcoral')

    # Add the value of each bar on top of the bar
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 10, yval, ha='center', va='bottom')

    plt.title('Total indexing duration based on trace length')
    plt.xlabel('Trace length')
    plt.ylabel('Indexing time (s)')
    plt.tight_layout()  # Adjust the layout to not cut off labels
    plt.savefig('plots/init_duration_per_trace_length.eps',format='eps')   
    
    
def plot_durations_per_unique_events(durations:list,unique_events:list):
    plt.figure(figsize=(10, 6))
    bars = plt.bar([str(x) for x in unique_events], durations, color='lightsalmon')

    # Add the value of each bar on top of the bar
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 10, yval, ha='center', va='bottom')

    plt.title('Total indexing duration based on unique events')
    plt.xlabel('Unique Events')
    plt.ylabel('Indexing time (s)')
    plt.tight_layout()  # Adjust the layout to not cut off labels
    plt.savefig('plots/init_duration_per_unique_events.eps',format='eps')     