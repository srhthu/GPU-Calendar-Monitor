"""
The daemon periodicly collect the node status and aggregate them in a dict.

We support loading of more keys in the node status dict.

Node status format:
    hostname
    last_update
    ips: List[Tuple[str, str]] where tuple is of interface and ip addr
    gpus: List of gpu info in dict:
        index: int
        name: str
        use_mem: int
        tot_mem: int
        utilize: int, percent
        temp: int
        users: List of process info in dict:
            pid: int
            username: str
            mem(MiB): int
            command: str
"""
import time
from threading import Thread, Lock
from typing import List, Tuple, Dict, Any
from datetime import datetime
from copy import deepcopy

from next_cluster.utils.gpu_status import (
    get_gpu_serial, get_gpu_stat, GPU_STAT, get_gpu_process
)
from next_cluster.utils.net_status import get_hostname, get_if_ip

class NodeStat:
    """
    Maintain the node status and run as a daemon. 
    
    Support flexible loading of more status information.
    """

    EXTRA_KEY_FUNC_MAP = {
        'ips': get_if_ip
    }

    def __init__(self, interval = 4, interval_proc = 10, extra_keys = ['ips']):
        """
        Args:
            interval: refresh interval (seconds) of general information
            interval_proc: refresh interval (seconds) of gpu process information
            extra_keys: List of key names of information that you want to include 
                in the status dict. You have to implement the function to get the 
                information. Here, just give an example of ip addresses
        """
        self._status = {'hostname': None,
                        'last_update': None,
                        'gpus': []}
        self.extra_keys = extra_keys
        for key in extra_keys:
            self._status[key] = None
        
        # the dict to host gpu process information returned by get_gpu_process
        self._gpu_proc_status: Dict[int, List[Dict[str, Any]]] = {}

        self.interval = interval 
        self.interval_proc = interval_proc

        self.serial_map: Dict[str, int] = get_gpu_serial()

        self.th_referesh = Thread(target = self.daemon_func, name = 'th_referesh')
        # Thread to update gpu process information
        self.th_proc = Thread(target = self.daemon_proc_func, name = 'th_proc')

        # set daemon thread that will exit when main thread is exiting.
        self.th_referesh.daemon = True
        self.th_proc.daemon = True
    
    def start(self):
        self.th_referesh.start()
        self.th_proc.start()

    def referesh(self):
        """Update node general information and gpu usages excluding gpu processes"""
        self._status['hostname'] = get_hostname()
        self._status['last_update'] = datetime.now().isoformat()
        self._status['gpus'] = [k.to_dict() for k in get_gpu_stat()]

        # Get the information of extra keys
        for key in self.extra_keys:
            self._status[key] = self.EXTRA_KEY_FUNC_MAP[key]()
    
    def daemon_func(self):
        """THe daemon to periodically referesh device infomation"""
        print(f'Start monitor daemon')
        while True:
            self.referesh()
            time.sleep(self.interval)
    
    def daemon_proc_func(self):
        """The daemon to update process information"""
        print('Start process monitor daemon')
        while True:
            self._gpu_proc_status = get_gpu_process(self.serial_map)
            time.sleep(self.interval_proc)    

    @property
    def status(self):
        """Return node status in dict. Assemble gpu process information into the status"""
        status = deepcopy(self._status)
        for gpu in status['gpus']:
            idx = gpu['index']
            gpu['users'] = self._gpu_proc_status.get(idx, [])

        return status

if __name__ == '__main__':
    import json

    n_stat = NodeStat(interval_proc = 4)
    n_stat.start()

    time.sleep(6)
    print(json.dumps(n_stat.status, indent = 4))
