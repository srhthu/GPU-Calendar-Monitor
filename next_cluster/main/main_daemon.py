# coding=utf-8
# © 2024 SHUI Ruihao <shuirh2019@gmail.com>
# All rights reserved.
"""
The daemon of main node to periodically collect nodes and calendar status and aggregate the information into a dict.

 ---- Assemble
    1. merge booking df -> Determin book_code
    2. determin user_code
    3. add calendar
"""

# code
## booking error code
GOOD_BOOK = 0
INVALID_BOOK_INFO = 1
EXCEED_MAX_BOOK_GPU = 2
EXCEED_MAX_BOOK_DAY = 3

## user code
GOOD_USER = 0
NO_BOOK_USER = 1

from typing import Optional, Union, Dict, Any, Tuple, List
import json
from collections import OrderedDict,defaultdict
import time
import re
from pathlib import Path
from datetime import date, datetime
from threading import Thread, Lock
import pandas as pd
import requests
from copy import deepcopy

from next_cluster.utils.teamup import get_bookings, translate_next
# from next_cluster.utils import get_linux_users

def get_linux_users():
    with open('/etc/passwd') as f:
        lines = [k.split(':') for k in f]
    users = [line[0] for line in lines]
    return users

class Cluster:
    """
    Hold status of all servers and check user booking legality.

    Args:
        host_data: a list of host info in dict: nickname, ip
    
    Status information format
        Node data (fetch from each node). * denote keys added by this class.
        {
            "hostname": "next-asus-01"
            "last_update": "2022-09-10T22:40:09.304060"
            "ips": List[Tuple[str, str]] where tuple is of interface and ip addr
            "gpus": list of gpu status, include
                "index"
                "name"
                "use_mem"
                "tot_mem"
                "utilize"
                "temp"
                "users": dict of process info
                    "username"
                    "pid": int
                    "mem(MiB)": int
                    "command"
                    *"user_code": 0 or 1
                *"calendar": Nested list:
                    Day: List of (title, who, error_code)
            *"version": gpu brand
            *"status": `bool`
        }
        
        Cluster Status: a dict of following keys:
            date_list
            calendar_status: (`bool`)
            Nodes: List of node data described above
            "illegal_users"
    """
    MAX_GPU_PER_USER = 4
    MAX_DAYS_PER_GPU = 3

    def __init__(
            self,
            host_data: List[dict],
            port = 7080, # port of the client flask api
            passwd = None,
            add_calendar: bool = True,
            user_list = 'user_list.txt',
            teamup_ids: List[str] = [],
            num_days = 5,
            name_translate = None,
            node_wait = 4,
            node_expire_time = 60,
            cal_wait = 10,
            dur_book_update = 5
        ):
        self.host_data = host_data
        self.port = port
        self.passwd = passwd
        self.add_calendar = add_calendar
        self.teamup_ids = teamup_ids
        self.num_days = num_days
        self.name_translate = name_translate
        self.node_wait = node_wait
        self.node_expire_time = node_expire_time
        self.cal_wait = cal_wait
        self.dur_book_update = dur_book_update

        self._cluster_stat = {}
        self._linux_users = []
        self.book_dt: Dict[str, pd.DataFrame] = {} 
        self.book_df: pd.DataFrame = None

        self.lock = Lock()
        self.date_list = None # calendar dates, list of "xxx xx xx"
        self.calendar_dt = None # calendar updating time
        
        self.nodes: Dict[str, Dict] = {h['nickname']:None for h in self.host_data} # map from hostname to Node data
        

        self.init_user_info(user_list)
        self.init_calendar_thread()
        self.init_fetch_thread()
        self.check_thread = Thread(target = self.daemon_check_and_update, 
                                       name = 'booking check')
        self.check_thread.daemon = True
        self.start_threads(self._cal_threads)
        self.start_threads(self._node_threads)
        self.check_thread.start()
    
    def init_user_info(self, filename):
        if Path(filename).exists():
            users = [k.split()[0] for k in open(filename).readlines()]
        else:
            print('No user list provided. Default to all linux users in /etc/passwd')
            users = get_linux_users()
        self._linux_users = users
    
    def init_calendar_thread(self):
        if self.add_calendar:
            self._cal_threads = [Thread(target = self.daemon_fetch_calendar, 
                                        args = (tid,), 
                                        name = f'calendar {i}') 
                                    for i, tid in enumerate(self.teamup_ids)]
        else:
            self._cal_threads = []
        for th in self._cal_threads:
            th.daemon = True

    def init_fetch_thread(self):
        self._node_threads = [Thread(target = self.daemon_fetch_node,
                                     args = (h, ),
                                     name = f'fetch {h["nickname"]}')
                                for h in self.host_data]
        for th in self._node_threads:
            th.daemon = True
        
    def start_threads(self, threads):
        for th in threads:
            th.start()

    def daemon_fetch_calendar(self, teamup_id):
        print(f'Enter calendar: {teamup_id}')
        while True:
            try:
                cal_data, self.date_list = get_bookings(teamup_id, 
                                                        self.num_days,
                                                        translate_next)
            except Exception as e:
                print(f'Calendar {teamup_id} fail {e}')
                time.sleep(3)
            else:
                lock = Lock()
                lock.acquire()
                self.calendar_dt = time.time()
                cal_data = cal_data.reset_index(drop = True)
                self.book_dt[teamup_id] = cal_data
                lock.release()
                time.sleep(self.cal_wait)

    def daemon_fetch_node(self, host_d: dict):
        # addr = host if host[0].isdigit() else host + '.' + self.domain
        host = host_d['nickname']
        addr = host_d['ip']
        while True:
            try:
                res = requests.post(f'http://{addr}:{self.port}/get-status', 
                                    json = {'passwd': self.passwd},
                                    timeout = 3)
                data = res.json()
                # print(f'Fetch {host}: successful')
            except Exception as e:
                print(f'Fetch {host}: no response.{repr(e)}')
                data = None
            self.lock.acquire()
            if data is not None:
                data['status'] = True
                self.nodes[host] = data
            elif self.nodes[host] is not None:
                q_time = datetime.fromisoformat(self.nodes[host]['last_update'])
                dur = (datetime.now() - q_time).total_seconds()
                self.nodes[host]['status'] = (dur <= self.node_expire_time)
            
            self.lock.release()
            time.sleep(self.node_wait)
    
    def daemon_check_and_update(self):
        """Check legality and update status dict"""
        while True:
            time.sleep(self.dur_book_update)
            self.lock.acquire()
            if self.add_calendar:
                df = pd.concat(list(self.book_dt.values()), axis = 0).reset_index(drop = True)
            else:
                df = pd.DataFrame([], columns = 'title who day hostname index'.split())
            self.book_df = self.add_booking_check(df)
            self.update_user_code()
            self._cluster_stat = self.assemble()
            self.lock.release()
            
    def add_booking_check(self, df: pd.DataFrame):
        """Add booking error code column"""
        # df columns: title, who, hostname, index, day

        # Invalid booking = 1
        df['invalid_book'] = df['title'].apply(lambda k: k not in self._linux_users)

        # Exceed maximum gpu number
        # title -> whether violate
        group = df.groupby('title')
        u2ngpu = group.apply(lambda k: (k['hostname'] + k['index'].astype(str)).nunique())
        violate_ngpu = u2ngpu.apply(lambda k: k > self.MAX_GPU_PER_USER)
        violate_ngpu.name = 'violate_ngpu'

        if len(violate_ngpu) > 0:
            violate_ngpu = violate_ngpu.reset_index()

        # Exceed maximum days per gpu
        # (title, hostname, index) -> whether violate
        g2 = df.groupby(['title','hostname', 'index'])
        gpu2day = g2.apply(lambda k: k['day'].nunique())
        violate_day = gpu2day.apply(lambda k: k > self.MAX_DAYS_PER_GPU)
        violate_day.name = 'violate_day'
        if len(violate_day) > 0:
            violate_day = violate_day.reset_index()

        # Merge to original dataframe
        if len(violate_ngpu) > 0:
            df = df.merge(violate_ngpu, how = 'left', on = ['title'])
        else:
            df['violate_ngpu'] = [False] * len(df)

        if len(violate_day) > 0:
            df = df.merge(violate_day, how = 'left', on = ['title', 'hostname', 'index'])
        else:
            df['violate_day'] = [False] * len(df)

        # invalid book, violate max gpu, violate max day
        # True, *, * -> 1
        # False, True, * -> 2
        # False, False, True -> 3
        # False, False, False -> 0
        def status2code(inv_bk, vio_gpu, vio_day):
            args= [inv_bk, vio_gpu, vio_day]
            if any(args):
                return args.index(True) + 1
            else:
                return 0
        status_df = pd.concat([df.pop(k) for k in ['invalid_book', 'violate_ngpu', 'violate_day']], axis = 1)

        if self.add_calendar:
            if len(status_df) == 0:
                df['code'] = [0] * len(df)
            else:
                df['code'] = status_df.apply(lambda k: status2code(*k.tolist()), axis = 1)
        else:
            df['code'] = [0] * len(df)
        return df

    def update_user_code(self):
        """Based on booking info, update process user code"""
        df = self.book_df
        for host, node in self.nodes.items():
            if node is None or not node['status']:
                continue
            for gpu in node['gpus']:
                # get current day book
                condition = ((df['hostname'] == host) &
                             (df['index'] == gpu['index']) &
                             (df['day'] == 0)
                             & (df['code'] == 0))
                part = df[condition]['title'] + df[condition]['who']
                bname = ' '.join(part.unique())
                for proc in gpu['users']:
                    try:
                        proc['user_code'] = int(proc['username'] not in bname)
                    except:
                        print('Code error: ', proc['username'], bname)

    def _psudo_node(self, host):
        # get host gpu number from booking
        df = self.book_df
        all_ind = df[df['hostname'] == host]['index']
        if len(all_ind) > 0:
            n = all_ind.max().item() + 1
        else:
            n = 0
        gpus = [{'index': i,
                 'name': '',
                 'use_mem': 0,
                 'tot_mem': 100,
                 'utilize': 0,
                 'users': []} for i in range(n)]
        return {'hostname': host, 'status': False, 'gpus': gpus}

    def get_gpu_calendar(self, host, index):
        df = self.book_df
        bk_days = [[] for _ in range(len(self.date_list))]

        gpu_df: pd.DataFrame = df[(df['hostname'] == host) & (df['index'] == index)]
        if len(gpu_df) == 0:
            return bk_days
        
        day_gp = gpu_df.groupby('day')
        def _df2list(df):
            return df.apply(lambda r: r.tolist(), axis = 1).tolist()
        daybk_ser = day_gp[['title', 'who', 'code']].apply(_df2list)
        for day, bks in daybk_ser.items():
            bk_days[day] = bks
        return bk_days

    def assemble(self):
        """
        Assemble node status and booking information.
        Return cluster status dict.
        """
        status = OrderedDict()
        status['date_list'] = self.date_list
        status['calendar_status'] = self.calendar_status
        status['teamup_ids'] = self.teamup_ids
        status['Nodes'] = []
        illegal_users = set()
        ranked_hosts = self.rank_node(list(self.nodes.keys()))

        for host in ranked_hosts:
            node = self.nodes[host]
            if node is None:
                node = self._psudo_node(host)
            else:
                node = deepcopy(node)
            
            for gpu in node['gpus']:
                if self.add_calendar:
                    gpu['calendar'] = self.get_gpu_calendar(host, gpu['index'])
                gpu_illegal = [proc['username'] for proc in gpu['users'] if proc['user_code']]
                illegal_users.update(gpu_illegal)

            node['version'] = node['gpus'][0]['name'] if node['gpus'] else ''
            status['Nodes'].append(node)
        
        status['illegal_users'] = list(illegal_users)
        return status

        
    def get_status(self):
        """
        Return clauster status in JSON to frontend.
        """
        return self._cluster_stat

    @staticmethod
    def rank_node(hostnames):
        """
        Rank hosts. Customize for your preferences.
        """
        asus_hosts = []
        dgx_hosts = []
        other_hosts = []
        for k in hostnames:
            if 'asus' in k:
                asus_hosts.append(k)
            elif 'dgx' in k:
                dgx_hosts.append(k)
            else:
                other_hosts.append(k)
        
        return sorted(asus_hosts) + sorted(dgx_hosts) + sorted(other_hosts)

    @property
    def calendar_status(self):
        if self.calendar_dt and (time.time() - self.calendar_dt < 120):
            return True
        else:
            return False
                    


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--passwd')
    parser.add_argument('--teamup_ids', nargs = '+', default = [])

    args = parser.parse_args()

    host_data = [
        {
            'nickname': 'next-asus-02',
            'ip': 'next-asus-02.d2.comp.nus.edu.sg'
        },
        {
            'nickname': 'next-gpu4',
            'ip': 'next-gpu4.d2.comp.nus.edu.sg'
        }
    ]
    add_cld = len(args.teamup_ids) > 0
    c = Cluster(host_data, add_calendar = add_cld, teamup_ids=args.teamup_ids,
                passwd = args.passwd)
    time.sleep(8)
    print(json.dumps(c.get_status(), indent = 4))
    time.sleep(10)
    print(json.dumps(c.get_status(), indent = 4))
    exit()