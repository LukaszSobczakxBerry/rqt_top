#!/usr/bin/python3

import rospy
import rosnode
import threading
from rqt_top.nodes_processes_producer import NodesProcessesProducer
from dataclasses import dataclass, field
from typing import List
from std_msgs.msg import Float64
import psutil
import importlib

nvml_available = importlib.find_loader('nvitop') is not None
if nvml_available:
    from nvitop.core import Device, GpuProcess

jtop_available = importlib.find_loader('jtop') is not None
if jtop_available:
    from jtop import jtop


@dataclass
class NodeStats():
    """ dataclass with statistics per process """
    node_name: str = ""
    pid: int = 0
    cpu_usage: List[float] = field(default_factory=list)
    cores_number_usage: int = 0
    gpu_usage: List[float] = field(default_factory=list)
    memory_usage: List[float] = field(default_factory=list)
    gpu_memory_usage: List[float] = field(default_factory=list)

    def clear(self):
        self.cpu_usage.clear()
        self.gpu_usage.clear()
        self.memory_usage.clear()
        self.gpu_memory_usage.clear()


class TopNode():
    """ ROS node for publishing cpu/gpu/memory usage of each node on the machine/container """

    def __init__(self):
        self.debug = rospy.get_param('~debug', False)
        self.only_from_namespace = rospy.get_param('~only_from_namespace', False)
        self.update_interval = rospy.get_param('~update_interval', 5)
        self.measure_rate = rospy.get_param('~measure_rate', 5)
        self.enable_measurements_per_process = rospy.get_param('~enable_measurements_per_process', True)
        self.enable_overall_measurements = rospy.get_param('~enable_overall_measurements', True)
        self.enable_gpu_measurements = rospy.get_param('~enable_gpu_measurements', True)
        global nvml_available, jtop_available
        nvml_available = self.enable_gpu_measurements & nvml_available
        jtop_available = self.enable_gpu_measurements & jtop_available
        
        if not self.enable_measurements_per_process and not self.enable_overall_measurements:
            rospy.logwarn("all measurements disabled by parameters, no data will be provided")

        self.nodes_processes = NodesProcessesProducer()
        self.nodes_stats = dict()
        self.overall_stats = NodeStats()
        self.stats_publishers = dict()

        # initialize GPU device object when dedicated GPU is present
        if nvml_available:
            self.gpu_device = Device(0)

    def spin(self):
        r = rospy.Rate(self.measure_rate)
        last_update = rospy.Time(0)

        while not rospy.is_shutdown():
            self.accumulate_stats()
            # count and publish stats if needed
            if rospy.Time.now() - last_update > rospy.Duration.from_sec(self.update_interval):
                self.produce_stats()
                last_update = rospy.Time.now()
            r.sleep()

    def accumulate_stats(self):
        if self.enable_measurements_per_process:
            self.get_stats_for_processes()

        if self.enable_overall_measurements:
            self.get_overall_stats()

    def get_stats_for_processes(self):
        # get ros nodes to measuring usage
        if self.only_from_namespace:
            all_nodes_names = rosnode.get_node_names(rospy.get_namespace())
        else:
            all_nodes_names = rosnode.get_node_names()

        for node_name in all_nodes_names:
            # for each node get its process
            process = self.nodes_processes.get_node_process(node_name)
            if process is not False:
                # get statistics of the process
                process_attr = process.as_dict(attrs=['cpu_num', 'cpu_percent', 'memory_percent'])
                # create node stats if doesn't exist yet
                if process.pid not in self.nodes_stats:
                    self.nodes_stats[process.pid] = NodeStats(
                            node_name=node_name,
                            pid=process.pid,
                            cores_number_usage=process_attr['cpu_num']
                            )

                # store cpu/memory percent usage for averaging value
                self.nodes_stats[process.pid].cpu_usage.append(process_attr['cpu_percent'])
                self.nodes_stats[process.pid].memory_usage.append(process_attr['memory_percent'])

                # get gpu stats when dedicated GPU
                if nvml_available:
                    this_process = GpuProcess(process.pid, self.gpu_device)
                    this_process.update_gpu_status()
                    gpu_usage = this_process.gpu_sm_utilization()   # None when unavailable
                    self.nodes_stats[process.pid].gpu_usage.append(gpu_usage if type(gpu_usage) == float else 0.0)
                    gpu_mem = this_process.gpu_memory_percent()     # None when unavailable
                    self.nodes_stats[process.pid].gpu_memory_usage.append(gpu_mem if type(gpu_mem) == float else 0.0)

    def get_overall_stats(self):
        # store cpu/memory percent usage for averaging value, get number of logical CPUs
        self.overall_stats.cpu_usage.append(psutil.cpu_percent())
        self.overall_stats.cores_number_usage = psutil.cpu_count()
        mem = psutil.virtual_memory()
        self.overall_stats.memory_usage.append(mem.used/mem.total*100)

        # get gpu stats when dedicated GPU
        if nvml_available:
            gpu_usage = self.gpu_device.gpu_utilization()   # None when unavailable
            self.overall_stats.gpu_usage.append(gpu_usage if type(gpu_usage) == int else 0.0)
            gpu_mem = self.gpu_device.memory_percent()      # None when unavailable
            self.overall_stats.gpu_memory_usage.append(gpu_mem if type(gpu_mem) == float else 0.0)

        # get gpu stats when jetson
        if jtop_available:
            with jtop() as jetson:
                self.overall_stats.gpu_usage.append(jetson.gpu['val'])

    def produce_stats(self):
        if self.enable_measurements_per_process:
            # measure average usage per each ROS node
            for pid in self.nodes_stats:
                # measure and publish usage statistics per process
                node_stats = self.nodes_stats[pid]
                self.measure_publish_stats(node_stats, node_stats.node_name + "/stats")

                # clear data accumulated for measuring average values
                self.nodes_stats[pid].clear()

        if self.enable_overall_measurements:
            # measure and publish overall usage statistics
            self.measure_publish_stats(self.overall_stats, "stats")
            # clear data accumulated for measuring average values
            self.overall_stats = NodeStats()

    def measure_publish_stats(self, stats, topic_prefix):
        # get avg values from arrays with measurements
        avg_cpu_usage = self.get_average(stats.cpu_usage)
        avg_memory_usage = self.get_average(stats.memory_usage)
        avg_gpu_usage = self.get_average(stats.gpu_usage)
        avg_gpu_memory_usage = self.get_average(stats.gpu_memory_usage)

        # publish avg usage stats
        self.publish(topic_prefix + "/cpu_usage", avg_cpu_usage)
        self.publish(topic_prefix + "/memory_usage", avg_memory_usage)
        self.publish(topic_prefix + "/gpu_usage", avg_gpu_usage)
        self.publish(topic_prefix + "/gpu_memory_usage", avg_gpu_memory_usage)

        if self.debug:
            rospy.loginfo("{} ({}): cpu={:0.2f}%, cores={}, gpu={:0.2f}%, mem={:0.2f}%, gpu_mem={:0.2f}%".format(
                stats.node_name, stats.pid, avg_cpu_usage, stats.cores_number_usage, avg_gpu_usage, avg_memory_usage, avg_gpu_memory_usage))

    def get_average(self, measurements):
        if len(measurements) > 0:
            return sum(measurements) / len(measurements)
        else:
            return -1    # -1 when usage statistic not available

    def publish(self, topic, value):
        self.get_publisher(topic).publish(value)

    def get_publisher(self, topic):
        if topic not in self.stats_publishers:
            self.stats_publishers[topic] = rospy.Publisher(topic, Float64, queue_size=1, latch=True)

        return self.stats_publishers[topic]

    def shutdown(self):
        for k in list(self.stats_publishers.keys()):
            self.stats_publishers[k].unregister()
            del self.stats_publishers[k]


def main(args=None):
    rospy.init_node('top_node')

    top_node = TopNode()

    try:
        top_node.spin()
    except rospy.ROSInterruptException:
        pass
    except Exception as ex:
        rospy.logerr("Top node caught an exception! Details: {}".format(ex))
    finally:
        top_node.shutdown()


if __name__ == '__main__':
    main()
