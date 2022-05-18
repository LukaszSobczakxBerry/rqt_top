#!/usr/bin/python3

import csv
from typing import List
from dataclasses import dataclass, field
import collections

@dataclass
class NodeStats():
    sum: float = 0.0
    avg: float = 0.0
    min: float = 600.0
    max: float = 0.0
    dev: float = 0.0
    measurements: List[float] = field(default_factory=list)

filename = "docker_stats_results/driving_offic_min_aut_docker-stats-2022-05-11-22-44-58.csv"
# filename = "docker_stats_results/driving-no-autonomy-docker-stats-2022-05-11-17-09-53.csv"
# filename = "docker_stats_results/static-no-active-node-docker-stats-2022-05-11-16-15-11.csv"
# filename = "docker_stats_results/static-active-node-docker-stats-2022-05-11-16-20-40.csv"
nodes_stats = dict()
metric_id = 1   # 1 - cpu, 2 - memory

def main(args=None):
    with open(filename, newline='') as f:
        reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
        for row in reader:
            fixed_row = ' '.join(row[0].split()).replace('%', '').split()
            if fixed_row[0] != 'NAME':
                if fixed_row[0] not in nodes_stats:
                    nodes_stats[fixed_row[0]] = NodeStats()
                metric = float(fixed_row[metric_id])
                stats = nodes_stats[fixed_row[0]]
                stats.sum += metric
                if metric > 0.01:
                    stats.min = min(stats.min, metric)
                stats.max = max(stats.max, metric)
                stats.measurements.append(metric)
                nodes_stats[fixed_row[0]] = stats

    overall_avg_usage = 0.0
    overall_max_usage = 0.0
    overall_min_usage = 0.0
    for key, value in nodes_stats.items():
        nodes_stats[key].avg = value.sum / len(value.measurements)
        nodes_stats[key].dev = nodes_stats[key].max - nodes_stats[key].avg
        overall_avg_usage += value.avg
        if (value.min >= 600.0):
            value.min = 0.0
        overall_min_usage += value.min
        overall_max_usage += value.max
    sorted_stats = collections.OrderedDict(sorted(nodes_stats.items(), key=lambda item: item[1].avg))
    print("overall: avg={:.3f}, min={:.3f}, max={:.3f}".format(overall_avg_usage, overall_min_usage, overall_max_usage))
    print("exact stats for {} containers:".format(len(sorted_stats)))
    for key, value in sorted_stats.items():
        if key == "coco-one-docker-cloudwatchlogs":
            print(value.measurements)
        print("{}: avg={:.3f}, min={:.3f}, max={:.3f}, dev={:.3f} ({})".format(key, value.avg, value.min, value.max, value.dev, len(value.measurements)))


if __name__ == '__main__':
    main()
