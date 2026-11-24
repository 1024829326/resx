# encoding=utf8
import os
import re
import sys
import time
import enum
import traceback
from typing import List, Generator, Dict, Optional
from itertools import groupby
from functools import lru_cache
import netifaces
from datetime import datetime, date


# 列表分段
def split_parts(inputs: List, sz: int) -> Generator[List, None, None]:
    buf = []
    for x in inputs:
        buf.append(x)
        if len(buf) == sz:
            yield list(buf)
            buf.clear()
    if len(buf) > 0:
        yield list(buf)


def update_dict_value(d: Dict, k, old, new, force=False):
    if (force and k not in d) or (k in d and d[k] == old):
        d[k] = new


# refer: https://stackoverflow.com/questions/29645415/python-zip-by-key
def zip_by_key(lst0: List, lst1: List, key_func=None):
    lst0 = list((x, 0) for x in lst0)
    lst1 = list((x, 1) for x in lst1)
    key_func = (lambda x: x[0][0]) if not key_func else (lambda x: key_func(x[0]))
    for k, v in groupby(sorted(lst0 + lst1, key=key_func), key_func):
        v = list(v)
        if len(v) == 1:
            if v[0][1] == 0:
                yield v[0][0], None
            else:
                yield None, v[0][0]
        elif v[0][1] == 0:
            yield v[0][0], v[1][0]
        else:
            yield v[1][0], v[0][0]


# 获取当前设备的网卡IP，这里假设机器只有一个IP
# https://stackoverflow.com/questions/24196932/how-can-i-get-the-ip-address-from-nic-in-python
@lru_cache(None)
def get_my_ip() -> Optional[str]:
    ip = None
    for if_name in netifaces.interfaces():
        interface = netifaces.ifaddresses(if_name)
        if netifaces.AF_INET not in interface:
            continue
        if len(interface[netifaces.AF_INET]) == 0:
            continue
        ip = interface[netifaces.AF_INET][0].get('addr', None)
        if not ip:
            continue
        if ip != '127.0.0.1':
            break
    print(f'get_ip >> {ip}')
    return ip


class ResXEnv(enum.Enum):
    ONLINE = 'online'  # 容器线上集群
    YUFA = 'yufa'  # 容器预发集群
    DEV = 'dev'  # 容器开发集群
    TEST = 'test'  # 容器测试集群
    LOCAL = 'local'  # 本机、本地


def get_env(env=None) -> ResXEnv:
    if env:
        return ResXEnv(env)
    # 获取环境变量
    if 'POD_ENV' in os.environ:
        return ResXEnv(os.environ.get('POD_ENV'))
    return ResXEnv.LOCAL


def mysql_equal(v_db, v) -> bool:
    if isinstance(v_db, float):
        return abs(v_db-v) < 1e-6
    return v == v_db


def date2str(d: date) -> Optional[str]:
    if d is None:
        return None
    dt = datetime.combine(d, datetime.min.time())
    return dt.strftime('%Y-%m-%d')


def to_date(o) -> date:
    dt = to_datetime(o)
    return None if dt is None else dt.date()


def datetime2str(dt: datetime) -> Optional[str]:
    if dt is None:
        return None
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def to_datetime(o) -> Optional[datetime]:
    if o is None:
        return None
    if isinstance(o, float) or isinstance(o, int):
        # 认为是时间戳 time.time() 的值
        if o < 2667975961.8920162:  # in secs
            return datetime.fromtimestamp(o)
        else:  # in micro-secs
            return datetime.fromtimestamp(o / 1e3)
    if isinstance(o, datetime):
        return o
    if isinstance(o, date):
        return datetime(o.year, o.month, o.day)
    if isinstance(o, str):
        if o == '':
            return None
        if o == '0000-00-00 00:00:00':
            return None
        if o == '0000-00-00':
            return None
        mo = re.fullmatch(r'(\d{4}).?(\d{1,2}).?(\d{1,2})', o)
        if mo:
            # 2022/11/01
            year, month, day = [int(x) for x in mo.groups()]
            return datetime(year, month, day)
        mo = re.fullmatch(r'(\d{4})年(\d{1,2})月(\d{1,2})日', o)
        if mo:
            # 2018年08月24日
            year, month, day = [int(x) for x in mo.groups()]
            return datetime(year, month, day)
        if re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', o):
            # 2022-01-09 14:56:57
            return datetime.strptime(o, '%Y-%m-%d %H:%M:%S')
        if re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+', o):
            # 2022-11-09 14:56:57.475718
            return datetime.strptime(o, '%Y-%m-%d %H:%M:%S.%f')
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', o):
            # 2022-11-09T14:56:57
            return datetime.strptime(o, '%Y-%m-%dT%H:%M:%S')
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+', o):
            # 2022-11-09T14:56:57.475718
            return datetime.strptime(o, '%Y-%m-%dT%H:%M:%S.%f')
    # logger.info(f'bad datetime {type(o)} [{o}], return None')
    return None


def cur_ts_sec() -> int:
    return int(time.time())


def get_stack_info():
    s = ''.join(traceback.format_stack()[:-1] + traceback.format_exception(*sys.exc_info())[1:])
    # s = s.replace('\n', '').replace('\r', ' ')
    return s