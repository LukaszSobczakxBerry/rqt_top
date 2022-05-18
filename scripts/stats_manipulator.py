#!/usr/bin/python3

import rospy
from std_msgs.msg import Float64
from typing import List
from dataclasses import dataclass, field
import collections


@dataclass
class NodeStats():
    sum: float = 0.0
    avg: float = 0.0
    min: float = 0.0
    max: float = 0.0
    dev: float = 0.0
    measurements: List[float] = field(default_factory=list)


class StatsAnaliser():
    def __init__(self, stats_prefix='/stats/', type=Float64):
        self.stats_prefix = stats_prefix
        self.type = type
        self.topics = []
        self.subscibers = dict()
        self.nodes_stats = dict()

    def get_stats_topics(self, stats_prefix):
        return [topic for topic in rospy.get_published_topics() if stats_prefix in topic[0]]

    def callback(self, msg, topic):
        if topic not in self.nodes_stats:
            self.nodes_stats[topic] = NodeStats()
        if msg.data > 0.0005:
            self.nodes_stats[topic].sum += msg.data
            if self.nodes_stats[topic].min == 0.0:
                self.nodes_stats[topic].min = msg.data
            else:
                self.nodes_stats[topic].min = min(msg.data, self.nodes_stats[topic].min)
            self.nodes_stats[topic].max = max(msg.data, self.nodes_stats[topic].max)
            self.nodes_stats[topic].measurements.append(msg.data)

    def spin(self):
        r = rospy.Rate(1)
        while not rospy.is_shutdown():
            topics = self.get_stats_topics(self.stats_prefix)
            if len(self.topics) != len(topics):
                for topic in topics:
                    if not topic[0] in self.subscibers:
                        self.subscibers[topic[0]] = rospy.Subscriber(topic[0], self.type, self.callback, (topic[0]))
            r.sleep()

    def summary(self):
        overall_avg_usage = 0.0
        overall_max_usage = 0.0
        for key, value in self.nodes_stats.items():
            self.nodes_stats[key].avg = value.sum / len(value.measurements)
            self.nodes_stats[key].dev = self.nodes_stats[key].max - self.nodes_stats[key].avg
            overall_avg_usage += value.avg
            overall_max_usage += value.max
        sorted_stats = collections.OrderedDict(sorted(self.nodes_stats.items(), key=lambda item: item[1].avg))
        rospy.loginfo("overall: avg={:.3f}, max={:.3f}".format(overall_avg_usage, overall_max_usage))
        rospy.loginfo("exact stats for {} nodes:".format(len(sorted_stats)))
        for key, value in sorted_stats.items():
            print("{}: avg={:.3f}, min={:.3f}, max={:.3f}, dev={:.3f} ({})".format(key.split('/')[-3], value.avg, value.min, value.max, value.dev, len(value.measurements)))


def main(args=None):
    rospy.init_node('stats_manipulator')

    stats_manipulator = StatsAnaliser(stats_prefix='/stats/cpu_usage')

    try:
        stats_manipulator.spin()
    except rospy.ROSInterruptException:
        pass
    except Exception as ex:
        rospy.logerr("stats_manipulator caught an exception! Details: {}".format(ex))
    finally:
        stats_manipulator.summary()


if __name__ == '__main__':
    main()
