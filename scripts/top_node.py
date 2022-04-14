#!/usr/bin/python3

import rospy
import threading
from rqt_top.node_info import NodeInfo
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

    NODE_FIELDS = ['pid', 'get_cpu_percent', 'get_cpu_num', 'get_memory_percent']

    def __init__(self):
        self.debug = rospy.get_param('~debug', False)
        self.only_from_namespace = rospy.get_param('~only_from_namespace', False)
        self.update_interval = rospy.get_param('~update_interval', 5)
        self.measure_rate = rospy.get_param('~measure_rate', 5)
        self.enable_measurements_per_process = rospy.get_param('~enable_measurements_per_process', True)
        self.enable_overall_measurements = rospy.get_param('~enable_overall_measurements', True)
        
        if not self.enable_measurements_per_process and not self.enable_overall_measurements:
            rospy.logwarn("all measurements disabled by parameters, no data will be provided")

        # start timer for updating usage statistics with update_interval
        rospy.Timer(rospy.Duration(self.update_interval), self.produce_stats)

        self.node_info = NodeInfo()
        self.nodes = dict()
        self.stats_publishers = dict()
        self.overall_stats = NodeStats()
        self.mtx = threading.Lock()
        if nvml_available:
            self.gpu_device = Device(0)

    def accumulate_stats(self):
        with self.mtx:
            if self.enable_measurements_per_process:
                self.get_stats_for_processes()

            if self.enable_overall_measurements:
                self.get_overall_stats()

    def get_stats_for_processes(self):
        infos = self.node_info.get_all_node_fields(self.NODE_FIELDS)
        for nx, info in enumerate(infos):
            self.store_node_info(info)

    def get_overall_stats(self):
        self.overall_stats.cpu_usage.append(psutil.cpu_percent())
        self.overall_stats.cores_number_usage = psutil.cpu_count()
        mem = psutil.virtual_memory()
        self.overall_stats.memory_usage.append(mem.used/mem.total*100)

        if nvml_available:
            gpu_usage = self.gpu_device.gpu_utilization()
            self.overall_stats.gpu_usage.append(gpu_usage if type(gpu_usage) == int else 0.0)
            gpu_mem = self.gpu_device.memory_percent()
            self.overall_stats.gpu_memory_usage.append(gpu_mem if type(gpu_mem) == float else 0.0)

        if jtop_available:
            with jtop() as jetson:
                self.overall_stats.gpu_usage.append(jetson.gpu['val'])

    def store_node_info(self, node_info):
        if node_info['pid'] not in self.nodes:
            if self.only_from_namespace and not node_info['node_name'].startswith(rospy.get_namespace()):
                return
            node_stats = NodeStats(
                node_name=node_info['node_name'],
                pid=node_info['pid'],
                cores_number_usage=node_info['cpu_num'])
            self.nodes[node_stats.pid] = node_stats

        self.nodes[node_info['pid']].cpu_usage.append(node_info['cpu_percent'])
        self.nodes[node_info['pid']].memory_usage.append(node_info['memory_percent'])

        if nvml_available:
            this_process = GpuProcess(node_info['pid'], self.gpu_device)
            this_process.update_gpu_status()
            gpu_usage = this_process.gpu_sm_utilization()
            self.nodes[node_info['pid']].gpu_usage.append(gpu_usage if type(gpu_usage) == float else 0.0)
            gpu_mem = this_process.gpu_memory_percent()
            self.nodes[node_info['pid']].gpu_memory_usage.append(gpu_mem if type(gpu_mem) == float else 0.0)

    def spin(self):
        r = rospy.Rate(self.measure_rate)
        while not rospy.is_shutdown():
            self.accumulate_stats()
            r.sleep()

    def get_average(self, measurements):
        if len(measurements) > 0:
            return sum(measurements) / len(measurements)
        else:
            return -1    # -1 when usage statistic not available

    def produce_stats(self, event):
        with self.mtx:
            if self.enable_measurements_per_process:
                # measure average usage per each ROS node
                for pid in self.nodes:
                    node_stats = self.nodes[pid]

                    # get avg values from arrays with measurements
                    avg_cpu_usage = self.get_average(node_stats.cpu_usage)
                    avg_memory_usage = self.get_average(node_stats.memory_usage)
                    avg_gpu_usage = self.get_average(node_stats.gpu_usage)
                    avg_gpu_memory_usage = self.get_average(node_stats.gpu_memory_usage)

                    # publish avg usage stats
                    self.publish(node_stats.node_name + "/stats/cpu_usage", avg_cpu_usage)
                    self.publish(node_stats.node_name + "/stats/memory_usage", avg_memory_usage)
                    self.publish(node_stats.node_name + "/stats/gpu_usage", avg_gpu_usage)
                    self.publish(node_stats.node_name + "/stats/gpu_memory_usage", avg_gpu_memory_usage)

                    if self.debug:
                        rospy.loginfo("{} ({}): cpu={:0.2f}%, cores={}, gpu={:0.2f}%, mem={:0.2f}%, gpu_mem={:0.2f}%".format(
                            node_stats.node_name, node_stats.pid, avg_cpu_usage, node_stats.cores_number_usage, avg_gpu_usage, avg_memory_usage, avg_gpu_memory_usage))

                    # clear data accumulated for measuring average values
                    self.nodes[pid].clear()

            if self.enable_overall_measurements:
                # get avg values from arrays with measurements
                avg_cpu_usage = self.get_average(self.overall_stats.cpu_usage)
                avg_memory_usage = self.get_average(self.overall_stats.memory_usage)
                avg_gpu_usage = self.get_average(self.overall_stats.gpu_usage)
                avg_gpu_memory_usage = self.get_average(self.overall_stats.gpu_memory_usage)

                # publish avg usage stats
                self.publish("/stats/cpu_usage", avg_cpu_usage)
                self.publish("/stats/memory_usage", avg_memory_usage)
                self.publish("/stats/gpu_usage", avg_gpu_usage)
                self.publish("/stats/gpu_memory_usage", avg_gpu_memory_usage)

                if self.debug:
                    rospy.loginfo("overall usage: cpu={:0.2f}%, cores={}, gpu={:0.2f}%, memory={:0.2f}%, gpu_mem=%{:0.2f}".format(
                        avg_cpu_usage, self.overall_stats.cores_number_usage, avg_gpu_usage, avg_memory_usage, avg_gpu_memory_usage))

                # clear data accumulated for measuring average values
                self.overall_stats = NodeStats()

    def publish(self, topic, value):
        self.get_publisher(topic).publish(value)

    def get_publisher(self, topic):
        if topic not in self.stats_publishers:
            self.stats_publishers[topic] = rospy.Publisher(topic, Float64, queue_size=1)

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
        rospy.loginfo("Keyboard interrupt detected!")
    except Exception as ex:
        rospy.logerr("Exception caught! Details: {}".format(ex))
    finally:
        top_node.shutdown()


if __name__ == '__main__':
    main()
