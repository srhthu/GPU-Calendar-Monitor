"""Network status"""
import socket
import psutil

from typing import List, Tuple, Dict, Any

def get_hostname() -> str:
    return socket.gethostname()

def get_if_ip() -> List[Tuple[str, str]]:
    """Return all interface and their ip addresses"""
    if_ip_list: List[Tuple[str, str]] = []
    if2addrs = psutil.net_if_addrs()

    for ifname, ifstat in psutil.net_if_stats().items():
        if not ifstat.isup or any([k in ifname for k in ['lo', 'docker']]):
            continue
        # filter ipv4 address
        addrs = [k for k in if2addrs[ifname] if k.family == socket.AF_INET]
        if len(addrs) > 0:
            if_ip_list.append([ifname, addrs[0].address])
    if_ip_list.sort(key = lambda k: k[1])

    return if_ip_list