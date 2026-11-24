from typing import Dict, Generator
import time
from pydantic import Field
from concurrent.futures import Future
from threading import Lock
from collections import Counter
from resx.func import get_stack_info
from resx.tools import BoundedExecutor
from resx.base_model import BaseModel
from resx.log import setup_logger

logger = setup_logger(name=__name__)


# 一个多线程的多任务处理类 其他业务继承 用于简单的多任务处理
# 特点：
#   1、可迭代的任务分离 （支持按轮次的调度、以及常规的任务名单列表）
#   2、任务为dict类型，这里先不做对象化，待优化
#   3、支持任务唯一键并根据唯一键串行执行task_key、或者去重执行filter_task_seen（注意内存消耗）
#   4、任务返回值枚举，这里采用常量定义，是否有更好的方法 接近枚举？枚举无法继承
#   5、BoundedExecutor用于消费缓冲
class TaskContext(BaseModel):
    iid: int
    item: dict
    code: int = 0
    context: Dict = Field(default_factory=dict)
    ts: float = Field(default_factory=time.time)


class MultiTask(object):
    CODE_INITIAL = 0  # 初始值
    CODE_EXCEPTION = 90  # 业务代码抛出异常
    CODE_FAIL = 91  # 业务代码处理失败
    CODE_DUP = 9  # 业务代码已经成功处理此数据
    CODE_EXCLUDE = 8  # 业务代码排除之外的数据
    CODE_SUCCESS = 1  # 业务代码成功

    def __init__(self, worker_num=1, task_key=None, filter_task_seen=False):
        self.process_code_stat = Counter()
        self.worker_num = worker_num
        self.task_key = task_key
        self.filter_task_seen = filter_task_seen
        self.fs_lock = Lock()
        self.fs: Dict[Future, TaskContext] = dict()
        self.fs_task_running = set()
        self.fs_task_seen = set()

    # 一次获取多个任务 多用于爬虫场景下的tasks按批次调度 如果没有配置get_tasks_batch则会使用 get_task
    def get_tasks_batch(self) -> Generator[list[dict], None, None]:
        tasks = []
        for task in self.get_task():
            tasks.append(task)
            if len(tasks) >= 10:
                yield tasks
                tasks = []
        if len(tasks) > 0:
            yield tasks

    # 迭代器返回所有任务 一般的场景是任务量很大的时候
    def get_task(self) -> Generator[dict, None, None]:
        raise NotImplementedError()

    # 任务处理函数 无需返回值
    def process(self, tc: TaskContext):
        pass

    # refer to 什么情况下会被主线程执行
    def _process(self, tc: TaskContext):
        ts0 = time.time()
        logger.info(f'TASK START {tc.__dict__}')
        self.process(tc)
        ts1 = time.time()
        if ts1 - ts0 < 0.2:
            time.sleep(0.2)

    # 回调函数，在线程池内部执行
    def callback_fn(self, f: Future):
        with self.fs_lock:
            tc: TaskContext = self.fs[f]
            del self.fs[f]
            if self.task_key:
                self.fs_task_running.remove(tc.item[self.task_key])
        try:
            f.result()
        except Exception as e:
            logger.info(f'error process {tc}  e={e} {get_stack_info()}')
            tc.code = MultiTask.CODE_EXCEPTION
        if tc.code == MultiTask.CODE_INITIAL:
            tc.code = MultiTask.CODE_SUCCESS
        tc.ts = time.time() - tc.ts
        logger.info(f'TASK FINISH {tc.__dict__}')
        self.process_code_stat[tc.code] += 1

    def stat(self):
        logger.info(f'process_code_stat {self.process_code_stat}')

    def run(self):
        with BoundedExecutor(max_workers=self.worker_num, thread_name_prefix='p') as worker_pool:
            iid = 0
            for items in self.get_tasks_batch():
                logger.info(f'get get_tasks_batch return size {len(items)}')
                for item in items:
                    iid += 1
                    if iid % 100 == 0:
                        self.stat()
                    tc = TaskContext(item=item, iid=iid)
                    while True:
                        if self.task_key and tc.item[self.task_key] in self.fs_task_running:
                            logger.debug(f'TASK RUNNING, wait. {tc.item[self.task_key]} {tc.__dict__}')
                        elif self.task_key and self.filter_task_seen and tc.item[self.task_key] in self.fs_task_seen:
                            logger.info(f'TASK SEEN, ignore it. {tc.item[self.task_key]} {tc.__dict__}')
                            break
                        else:
                            future: Future = worker_pool.submit(self._process, tc)
                            future.add_done_callback(self.callback_fn)
                            with self.fs_lock:
                                self.fs[future] = tc
                                if self.task_key:
                                    self.fs_task_running.add(tc.item[self.task_key])
                                    if self.filter_task_seen:
                                        self.fs_task_seen.add(tc.item[self.task_key])
                            break
                        time.sleep(1)
        while len(self.fs) > 0:
            time.sleep(1)
        self.stat()
        return True
