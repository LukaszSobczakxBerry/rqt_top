import rospy
import rosnode
import psutil
try:
    from xmlrpc.client import ServerProxy
except ImportError:
    from xmlrpclib import ServerProxy
from socket import error as SocketError

class NodesProcessesProducer():
    """ class for storing ROS nodes_stats as processes """

    ID = '/NODEINFO'

    def __init__(self):
        self.processes = dict()

    def get_node_process(self, node_name, skip_cache=False):
        node_api = rosnode.get_api_uri(rospy.get_master(), node_name, skip_cache=skip_cache)
        try:
            code, msg, pid = ServerProxy(node_api[2]).getPid(self.ID)
            if node_name in self.processes:
                return self.processes[node_name]
            else:
                try:
                    p = psutil.Process(pid)
                    self.processes[node_name] = p
                    return p
                except:
                    return False
        except SocketError:
            if not skip_cache:
                return self.get_node_process(node_name, skip_cache=True)
            else:
                return False
