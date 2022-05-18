#!/usr/bin/python3

import csv
from typing import List
from dataclasses import dataclass, field
import collections

@dataclass
class NodeStats():
    name: str = ""
    sum: float = 0.0
    avg: float = 0.0
    min: float = 600.0
    max: float = 0.0
    dev: float = 0.0
    samples: int = 0
    measurements: List[float] = field(default_factory=list)
    seconds: List[int] = field(default_factory=list)

filename = "cpu_prod_bots_2week.csv"

bot_stats = dict()

def get_name_from_label(label):
    index = label.find('prod_C1')
    if index != -1:
        return label[index+5:index+11]
    return label

def main(args=None):
    with open(filename, newline='') as f:
        reader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_NONE)
        for i, row in enumerate(reader):    # row by row
            if i == 3:  # row with bot name
                for j, col in enumerate(row):   # cell by cell in row
                    if j > 0: # skip first cell
                        name = get_name_from_label(col)
                        bot_stats[name] = dict()
                keys = list(bot_stats.keys())   # keys (names) to list for indexing
            if i > 4:
                minute = i - 5
                for j, col in enumerate(row):   # cell by cell in row
                    if j > 0: # skip first cell
                        bot_stats[keys[j-1]][minute] = 0.0 if col == "" else float(col)
    print("{} bots, {} minutes".format(len(bot_stats), len(list(bot_stats.values())[0])))

    timeoutForZero = 30
    filtered_bots = dict()
    for key, value in bot_stats.items():
        lastMinute = 0
        lastNonZero = None
        usageRecord = []
        filtered_bots[key] = []
        for minute, usage in value.items():
            if usage > 0.0:
                lastNonZero = minute
            
            if lastNonZero is not None and minute - lastNonZero < timeoutForZero:
                usageRecord.append(usage)
            elif lastNonZero is not None and minute - lastNonZero >= timeoutForZero:
                if len(usageRecord) > timeoutForZero + 10:
                    filtered_bots[key].append(usageRecord)
                usageRecord = []

    running_bots = dict()
    running_bots_number = 0
    for key, value in filtered_bots.items():
        running_bots[key] = []
        if len(value) > 0:
            for v in value:
                running = [i for i in v if i > 55.0]
                if len(running) > 0 and len(running) / float(len(v)) > 0.05:
                    running_bots[key].append(running)
            if len(running_bots[key]) > 0:
                # print("{}: {}".format(key, len(running_bots[key])))
                running_bots_number += 1
    print("detected {} running bots".format(running_bots_number))

    final_bots = dict()
    for key, value in running_bots.items():
        usage = []
        if len(value) == 1:
            for v in value[0]:
                usage.append(v)
        elif len(value) > 1:
            for v in value:
                for el in v:
                    usage.append(el)
        if len(usage) > 0:
            final_bots[key] = usage

    stats = dict()
    for key, value in final_bots.items():
        if len(value) > 100:
            node_stats = NodeStats()
            node_stats.avg = sum(value) / len(value)
            node_stats.max = max(value)
            node_stats.dev = node_stats.max - node_stats.avg
            node_stats.samples = len(value)
            stats[key] = node_stats
            print("{}: avg: {}, max: {}, dev: {}, ({})".format(key, node_stats.avg, node_stats.max, node_stats.dev, node_stats.samples))
    print("final {} bots".format(len(stats)))

    sorted_stats = collections.OrderedDict(sorted(stats.items(), key=lambda item: item[1].avg))

    header = ['name', 'avg', 'max', 'dev', 'samples']
    data = []
    with open('output.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for key, value in sorted_stats.items():
            data = [key, value.avg, value.max, value.dev, value.samples]
            writer.writerow(data)

def main2(args=None):
    with open(filename, newline='') as f:
        reader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_NONE)
        for i, row in enumerate(reader):
            if i == 3:  # row with bot name
                for j, col in enumerate(row):
                    if j > 0: # skip first column
                        bot_stats[j] = NodeStats()
                        bot_stats[j].name = get_name_from_label(col)
            if i > 4:   # cpu utilization
                for j, col in enumerate(row):
                    if j > 0: # skip first column
                        if col != "":   # skip empty values
                            bot_stats[j].sum += float(col)
                            bot_stats[j].min = min(bot_stats[j].min, float(col))
                            bot_stats[j].max = max(bot_stats[j].max, float(col))
                            bot_stats[j].measurements.append(float(col))
                            bot_stats[j].seconds.append(i-5)

    overall_avg_usage = 0.0
    overall_max_usage = 0.0
    overall_min_usage = 0.0
    for key, value in bot_stats.items():
        if len(value.measurements) > 0:
            bot_stats[key].avg = value.sum / len(value.measurements)
            bot_stats[key].dev = bot_stats[key].max - bot_stats[key].avg
        overall_avg_usage += value.avg
        if (value.min >= 600.0):
            value.min = 0.0
        overall_min_usage += value.min
        overall_max_usage += value.max
    sorted_stats = collections.OrderedDict(sorted(bot_stats.items(), key=lambda item: item[1].max))
    # print("exact stats for {} bots:".format(len(sorted_stats)))
    # for key, value in sorted_stats.items():
    #     print("{}: avg={:.3f}, min={:.3f}, max={:.3f}, dev={:.3f} ({})".format(value.name, value.avg, value.min, value.max, value.dev, len(value.measurements)))

    # remove bots which don't move and have few measurements
    filtered_bots = dict()
    for key, value in sorted_stats.items():
        if value.max > 55.0 and len(value.measurements) > 10:
            filtered_bots[value.name] = value
    
    delivery_bots = dict()
    delivery_thresh = 50
    print("filtered {} bots".format(len(filtered_bots)))
    for key, value in filtered_bots.items():
        if len(value.measurements) == len(value.seconds):
            running = False
            measurements = []
            avgs = []
            maxs = []
            times = []
            for sec in value.seconds:
                if sec + 3 > len(value.seconds):
                    break
                if not running and value.measurements[sec] > delivery_thresh and value.measurements[sec+1] > delivery_thresh and value.measurements[sec+2] > delivery_thresh:
                    running = True
                if running:
                    measurements.append(value.measurements[sec])
                    if value.measurements[sec] < delivery_thresh and value.measurements[sec+1] < delivery_thresh and value.measurements[sec+2] < delivery_thresh:
                        running = False
                        avgs.append(sum(measurements) / len(measurements))
                        maxs.append(max(measurements))
                        times.append(len(measurements))
                        measurements = []
            if(len(avgs) > 0):
                delivery_bots[key] = (times, avgs, maxs)
                # print("{}: times: {} avgs: {} max: {}".format(key, times, avgs, maxs))
    
    print("delivery bots: {}".format(len(delivery_bots)))
    final_bots = dict()
    for key, value in delivery_bots.items():
        times = []
        avgs = []
        maxs = []
        for i, t in enumerate(value[0]):
            if t > 30:
                times.append(value[0][i])
                avgs.append(value[1][i])
                maxs.append(value[2][i])
        if len(times) > 0:
            final_bots[key] = (times, avgs, maxs)

    print("final bots: {}".format(len(final_bots)))
    for key, value in final_bots.items():
        print("{}: times: {} avgs: {} max: {}".format(key, value[0], value[1], value[2]))

if __name__ == '__main__':
    main()
