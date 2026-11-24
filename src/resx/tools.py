# encoding=utf8

import time
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore
from resx.log import setup_logger

logger = setup_logger(name=__name__)


# https://gist.github.com/frankcleary/f97fe244ef54cd75278e521ea52a697a
class BoundedExecutor(ThreadPoolExecutor):
    """BoundedExecutor behaves as a ThreadPoolExecutor which will block on
    calls to submit() once the limit given as "bound" (max_workers*cache_factor) work items are queued for
    execution.
    """

    def __init__(self, max_workers: int = 1, cache_factor: int = 2, **kwargs):
        self.max_cache = max_workers * cache_factor
        self.semaphore = BoundedSemaphore(self.max_cache)
        self.name = kwargs.get('thread_name_prefix', 'thread')
        self.last_warning_ts = time.time() - 60 + 2
        super().__init__(max_workers=max_workers, **kwargs)

    # See concurrent.futures.Executor#submit
    def submit(self, fn, *args, **kwargs):
        self.semaphore.acquire()
        if getattr(self.semaphore, '_value') == 0:
            if time.time() - self.last_warning_ts > 60:
                logger.warning(f'executor full, max_cache={self.max_cache} name={self.name}, next warning in 60s')
                self.last_warning_ts = time.time()
        try:
            future = super().submit(fn, *args, **kwargs)
        except Exception as e:
            self.semaphore.release()
            raise e
        else:
            future.add_done_callback(lambda x: self.semaphore.release())
            return future
