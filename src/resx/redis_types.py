# encoding=utf8

import re
import time
import random
from datetime import datetime, timedelta
from typing import Type, Generator, List, TypeVar, Tuple
import redis
import threading
from resx.tunnel import TunnelMixin
from resx.func import get_my_ip, datetime2str, to_datetime
from resx.log import setup_logger

logger = setup_logger(name=__name__)

HashValueType = TypeVar('HashValueType', int, str, datetime)


class Redis(redis.Redis, TunnelMixin):
    def __init__(self, host='localhost', port=6379, password=None, db=0, tunnel=None, **kwargs):
        self.host, self.port, self.tunnel = host, port, tunnel
        self.mix()
        super().__init__(
            host=self.host,
            port=self.port,
            password=password,
            db=db,
            decode_responses=True,
            **kwargs,
        )


# 只接受datetime和str
class RedisHash(object):
    def __init__(self, name, value_type: type = int, **kwargs):
        self.redis = Redis(**kwargs)
        self.name: str = name
        self.value_type: Type = value_type

    def get(self, key):
        o = self.redis.hget(self.name, key=key)
        if o is None:
            return o
        if self.value_type == datetime:
            o = datetime.strptime(o, '%Y-%m-%d %H:%M:%S')
        if self.value_type == int:
            o = int(o)
        return o

    def get_all(self) -> dict[str, HashValueType]:
        data = self.redis.hgetall(self.name)
        for key, value in data.items():
            if self.value_type == datetime:
                data[key] = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            if self.value_type == int:
                data[key] = int(value)
        return data

    def set(self, key, value):
        assert isinstance(value, self.value_type)
        if isinstance(value, datetime):
            value = value.strftime('%Y-%m-%d %H:%M:%S')
        return self.redis.hset(self.name, key=key, value=value)

    def __len__(self):
        return self.redis.hlen(self.name)


# queue 基于redis zset or list
# 1 超过队列长度，阻塞N秒
# 2 支持插队
# 3 支持延迟队列 当生效时，强制使用zset
class RedisQueue(object):
    def __init__(self, name, max_length=1024, use_zset=True, delay_secs=0, **kwargs):
        self.ts_delta_1 = 1000000000
        self.name: str = name
        self.max_length: int = max_length
        self.use_zset = use_zset
        self.delay_secs = delay_secs
        if self.delay_secs > 0 and not self.use_zset:
            self.use_zset = True
            logger.info(f'force use zset when delay_secs > 0')
        self.redis = Redis(**kwargs)

    def generate(self, ) -> Generator[str, None, None]:
        while True:
            s = self.pop()
            if s is None:
                logger.warning('no data from %s, sleep for...', self.name)
                time.sleep(5)
                continue
            yield s

    def pop(self, ):
        if self.use_zset:
            data = self.redis.zpopmin(self.name, count=1)
            if len(data) == 0:
                return None
            value, score = data[0]
            if score < self.ts_delta_1:
                score += self.ts_delta_1
            delta_secs = time.time() - score
            if delta_secs < self.delay_secs:
                time.sleep(self.delay_secs - delta_secs)
                logger.info(f'sleep for delay delta_secs={delta_secs} value={value}')
            return data[0][0]
        else:
            data = self.redis.lpop(self.name)
        return data

    def push(self, value, realtime=False, wait=5.0):
        length = self.redis.zcard(self.name) if self.use_zset else self.redis.llen(self.name)
        max_length = 10000000 if realtime else self.max_length
        if length > max_length and wait > 0:
            logger.warning(f'sleep for length {length} greater than {max_length}')
            time.sleep(wait)
        if self.use_zset:
            score = time.time()
            if realtime:
                score -= self.ts_delta_1
            return self.redis.zadd(self.name, {value: score})
        else:
            push_func = self.redis.lpush if realtime else self.redis.rpush
            return push_func(self.name, value)


# 一个简单的注册中心
class RedisRegister(object):
    def __init__(self, name, db, ttl=120, **kwargs):
        self.redis = Redis(db=db, **kwargs)
        self.name = name
        self.ttl = ttl
        self.lock = self.redis.lock(name=f'{name}:lock', timeout=1.0)

    @staticmethod
    def _parse_task(task):
        tokens = task.split('#')
        task_name = tokens[0]
        task_params = tokens[1] if len(tokens) >= 3 else ''
        return task_name, task_params

    def register_task(self, known_tasks: List[str] = None) -> Tuple[str, str]:
        """
        获取一个任务， 同时具有更新心跳时间的功能
        任务名称 通过 task_name#task_param#task_id 或者 task_name#task_id 或者 task_name构成
        task_name 通过 known_tasks检测
        随机获取可选择的任务
        """
        thread_ident = f'{get_my_ip()}-{threading.current_thread().ident}'
        available_tasks = []
        with self.lock:
            for task, conf in self.redis.hgetall(self.name).items():
                task_name, task_params = self._parse_task(task)
                if known_tasks is not None and task_name not in known_tasks:  # unknown task, skip it
                    continue
                if conf == '':
                    available_tasks.append(task)
                    continue
                mo = re.fullmatch(r'([^ ]+) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', conf)
                if not mo:
                    logger.warning(f'bad conf value {conf}, need check!!!, skip task={task}')
                    continue
                thread_ident_task, ts = mo.groups()
                if thread_ident == thread_ident_task:  # 当前线程已经注册过 无需选取其他任务
                    self.redis.hset(self.name, task, f'{thread_ident} {datetime2str(datetime.now())}')
                    return task_name, task_params
                if to_datetime(ts) + timedelta(seconds=self.ttl) < datetime.now():  # 超时线程 先记录
                    available_tasks.append(task)
                    continue
            if len(available_tasks) > 0:
                task = random.choice(available_tasks)
                self.redis.hset(self.name, task, f'{thread_ident} {datetime2str(datetime.now())}')
                return self._parse_task(task)
            return '', ''
