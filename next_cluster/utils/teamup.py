"""
Get teamup calendar and return booking recoreds in DataFrame.
"""

from typing import Optional, Union, List, Dict, Tuple, Callable
import requests
import json
from bs4 import BeautifulSoup
import re
import sys
import pandas as pd
import datetime
import time

def get_calendar_id(teamup_id: str) -> Dict[int, Tuple[str, str]]:
    """
    Get the map from Teamup subcalendar_id to a tuple of (node name, gpu idx).

    Each gpu is viewed as a subcalendar by Teamup website

    Args:
        teamup_id: Teamup calendar id.

    Return:
        gpu_dt: a dict mapping subcalendar_id to [node, gpu_index]
            e.g., ["asus 4", "0"], ["dgx1 2", "7"]
    """
    url = "https://teamup.com/{}".format(teamup_id)
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    script_list = soup.find_all('script', src = None)

    reg = re.compile(r'var calendars =.*?(\[.*?\])', re.DOTALL)
    m = None
    for script in script_list:
        m = reg.search(script.text)
        if m is not None:
            break
    if m is None:
        raise RuntimeError('Calendar Error: calendar_id not found')
    calendar_id = json.loads(m.group(1))  # A list like [{"id": 12345, "name": "ASUS 1 > GPU 0"}]

    gpu_dt = {}  # the dictionary from subcalendar_id to gpu: (node, index)
    for cal_id in calendar_id:
        node, gpu_idx = cal_id['name'].split('>')   # the name is like 'DGX1 1 > GPU'
        node = ' '.join(node.split()).lower()       # remove extra space
        gpu_idx = int(re.search(r'[0-9]+', gpu_idx).group())
        gpu_dt[cal_id['id']] = [node, gpu_idx]

    return gpu_dt

def get_event(teamup_id: str, start_date:datetime.datetime, end_date:datetime.datetime
)->List[dict]:
    """
    Get bookings during the time span of [start_date, end_date].

    Each booking consists of one title (who), several gpus (subcalendar) and a time span.

    Args:
        teamup_id: Teamup calendar id
        start_date, end_date: should have the method of strftime

    Return:
        event_list: a list of event. Each event is a dict of:
            gpu_ids: a list of subcalendar_ids
            user: a tuple of (title, who)
            range: [start_day, end_day +1] (offset to start_date)
    """
    web_fmt = '%Y-%m-%d'
    url = 'https://teamup.com/{}/events'.format(teamup_id)
    payload = {
        'startDate': start_date.strftime(web_fmt),
        'endDate': end_date.strftime(web_fmt),
        'tz': 'Asia/Shanghai'
    }
    r = requests.get(url, params = payload)
    my_event = []
    for event in r.json()['events']:
        d = {}
        d['gpu_ids'] = event['subcalendar_ids']
        d['user'] = (event['title'], event['who'])
        e_start = datetime.datetime.strptime(event['start_dt'][:10], '%Y-%m-%d')
        e_end = datetime.datetime.strptime(event['end_dt'][:10], '%Y-%m-%d')
        offset_start = max((e_start - start_date).days, 0)
        offset_end = min((e_end - start_date).days, (end_date - start_date).days)
        d['range'] = [offset_start, offset_end+1]
        my_event.append(d)
    return my_event

def get_micro_events(teamup_id: str,
                     start_date:datetime.datetime,
                     end_date:datetime.datetime)-> pd.DataFrame:
    """
    Get micro bookings during <start_date, end_date>.
    A micro bokking is for one day one gpu one user.
        - title: (`str`)
        - who: (`str`)
        - day: (`int`), offset
        - gpu_id: (`str`)
    """
    events = get_event(teamup_id, start_date, end_date)
    for e in events:
        user = e.pop('user')
        e['title'], e['who'] = user
        st, ed = e.pop('range')
        e['day'] = list(range(st, ed))
        e['gpu_id'] = e.pop('gpu_ids')
    book_df = pd.DataFrame(events, columns = ['title', 'who', 'day', 'gpu_id']) # gpu_id, title, who, day
    book_df = book_df.explode('gpu_id')
    book_df = book_df.explode('day')
    book_df['day'] = book_df['day'].astype(int)
    return book_df

def get_bookings(teamup_id: str, time_span: int = 7, 
                 translate: Optional[Callable] = None
                 ) -> Tuple[pd.DataFrame, List[str]]:
    """
    Return micro bookings:
        - title
        - who
        - day
        - hostname: (`str`)
        - index: (`int`)
    """
    subcalendar_to_gpu = get_calendar_id(teamup_id)

    # customize your time zone here.
    singapore_zone = datetime.timezone(datetime.timedelta(hours = 8))
    utc_time = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)
    now = utc_time.astimezone(singapore_zone)  # local current time
    now = datetime.datetime(now.year, now.month, now.day)
    book_df = get_micro_events(teamup_id, now, now + datetime.timedelta(days=time_span-1))

    # convert calendar_gpu_id to hostname and index
    gpu_id_col = book_df.pop('gpu_id')
    if len(gpu_id_col) == 0:
        book_df['hostname'] = pd.Series([], dtype = 'str')
        book_df['index'] = pd.Series([], dtype = 'int64')
    else:
        book_df[['hostname', 'index']] = gpu_id_col.apply(
                                    lambda k: pd.Series(subcalendar_to_gpu[k]))
    if translate is not None:
        book_df['hostname'] = book_df['hostname'].apply(translate)
    book_df['index'] = book_df['index'].astype(int)
    
    date_list = [(now + datetime.timedelta(days=i)).strftime('%Y %m %d') for i in range(time_span)]
    return book_df, date_list

# Utilities for NExT
# Customize this function to map teamup node name to node hostname,
# if they are not identical.
def translate_next(node_name: str) -> str:
    """
    Translate from teamup node name to hostname.

    Example:
        "ASUS 1" -> "next-asus-01"
    """
    node_n, node_id = node_name.lower().split()
    table = {'asus': 'next-asus-0',
             'dgx1': 'next-dgx1-0',
             'node': 'next-gpu'}
    node_n = table.get(node_n, node_n)
    return node_n + node_id


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('teamup_id', 
                        help = 'Teamup id, i.e., https://teamup.com/<teamup_id>')
    parser.add_argument('--days', help = 'number of days.', default = 7, type = int)
    args = parser.parse_args()

    df, _ = get_bookings(args.teamup_id, args.days, translate_next)
    print(df.head(20))
    print(df.dtypes)