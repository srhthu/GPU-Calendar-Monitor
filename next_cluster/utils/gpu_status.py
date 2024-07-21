"""GPU status and occupied process information"""
import pynvml as N
from dataclasses import dataclass, field
from typing import List, Union, Dict, Optional, Any, Tuple
import subprocess
import pandas as pd
import psutil

@dataclass
class GPU_STAT:
    index: int
    name: str # GPU Brand name, e.g., NVIDIA RTX 3090
    use_mem: int # Used memory in MiB
    tot_mem: int # Total memory in MiB
    utilize: int # utilization percentage, e.g., 80 for 80% utilization
    temp: int # GPU Temperature
    users: List[Dict[str, Any]] = field(default_factory=list) 
    # Each element is a process information dict

    def __post_init__(self):
        # Change str type to int
        for key in ['index', 'use_mem', 'tot_mem', 'utilize', 'temp']:
            self.__dict__[key] = int(self.__dict__[key])
    
    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value
    
    def to_dict(self):
        return self.__dict__


def get_gpu_stat()->List[GPU_STAT]:
    """Use pynvml to get all GPUs status."""
    gpus = []
    N.nvmlInit()
    for gpu_idx in range(N.nvmlDeviceGetCount()):
        handle = N.nvmlDeviceGetHandleByIndex(gpu_idx)
        
        gname = N.nvmlDeviceGetName(handle)
        if isinstance(gname, bytes):
            gname = gname.decode('utf-8')
        info = N.nvmlDeviceGetMemoryInfo(handle)
        use_mem = int(info.used / 1024 / 1024)
        tot_mem = int(info.total / 1024 / 1024)
        info = N.nvmlDeviceGetUtilizationRates(handle)
        utilize = int(info.gpu)
        temp = N.nvmlDeviceGetTemperature(handle, 0)
        
        gpu = GPU_STAT(gpu_idx, gname, use_mem, tot_mem, utilize, temp)
        
        gpus.append(gpu)
    
    N.nvmlShutdown()
    return gpus

def get_gpu_serial()-> Dict[str, int]:
    """map from serial to index(int)"""
    ser_map = {}
    N.nvmlInit()
    for gpu_idx in range(N.nvmlDeviceGetCount()):
        handle = N.nvmlDeviceGetHandleByIndex(gpu_idx)
        r = N.nvmlDeviceGetSerial(handle)
        if isinstance(r, bytes):
            r = r.decode()
        ser_map[r] = gpu_idx
    N.nvmlShutdown()
    return ser_map

def get_proc_info(pid) -> Tuple[str, str]:
    """Return the username and command of a process with pid"""
    pid = int(pid)
    try:
        pro = psutil.Process(pid)
        with pro.oneshot():
            username = pro.username()
            command = ' '.join(pro.cmdline())[:500]
    except:
        print('Unknown pid: {}'.format(pid))
        username = None
        command = None
    return username, command

def get_gpu_process(serial_map: Optional[Dict[str, int]])->Dict[int, List[Dict[str, Any]]]:
    """
    Use nvidia-smi command to get information of processes occupying GPUs.

    Args:
        serial_map: dict from gpu serial number to gpu index. 
            It is usually provided to avoid repeatly getting it. If not, get it.
    """
    command = 'nvidia-smi --query-compute-apps=gpu_serial,pid,used_memory --format=csv'

    if serial_map is None:
        serial_map = get_gpu_serial()
    
    # Consistantly run the command.
    while True:
        try:
            p = subprocess.Popen(command, shell=True, close_fds=True, bufsize=-1,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            # To avoid frequent query with nvidia-smi, set a waiting time of 10 seconds
            p.wait(10)
            break
        except:
            continue
    
    df = pd.read_csv(p.stdout, dtype = str)
    # columns: gpu_serial, pid, used_gpu_memory [MiB]
    df.rename(lambda k: k.strip(), axis = 'columns', inplace = True)
    # print(df.columns)

    # Process columns
    df['pid'] = df['pid'].astype(int)
    df['idx'] = df['gpu_serial'].apply(lambda k: serial_map[k])
    df['mem(MiB)'] = df['used_gpu_memory [MiB]'].apply(lambda k: int(k.split()[0]))

    # Build a map from gpu index to a List of process status dicts
    gpu2procs = df.groupby('idx')[['pid', 'mem(MiB)']].apply(
                    lambda k: k.to_dict('records')).to_dict()
    for procs in gpu2procs.values():
        for proc in procs:
            proc['username'], proc['command'] = get_proc_info(proc['pid'])
    
    new_gpu2procs = {idx:[p for p in procs if p['username']] for idx,procs in gpu2procs.items()}

    return new_gpu2procs

