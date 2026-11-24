# encoding=utf8

import time
from datetime import datetime, timedelta
from abc import abstractmethod
from concurrent.futures import Future
from typing import List, Optional, Generator, Tuple
from concurrent.futures.thread import ThreadPoolExecutor
from resx.base_model import BaseModel
from resx.redis_types import RedisHash
from resx.mysql_dao import EntityType
from resx.mysql_dao import MySQLDao, PKType
from resx.log import setup_logger

logger = setup_logger(name=__name__)


# 分库分表Dao hold a list of MySQLDao
class MySQLShardingDao(object):
    def __init__(
            self,
            sharding_key='id',
            **kwargs,
    ):
        self.sharding_key = sharding_key
        self.mysql_dao_list: List[MySQLDao] = self.get_sharding_dao_list(**kwargs)
        assert len(self.mysql_dao_list) > 0
        self.entity_class = self.mysql_dao_list[0].entity_class

    # 给定v 给出 分库分表位置
    @classmethod
    @abstractmethod
    def do_sharding(cls, v) -> int:
        pass

    @classmethod
    @abstractmethod
    def get_sharding_dao_list(cls, **kwargs) -> List[MySQLDao]:
        pass

    def get(self, **kwargs) -> Optional[EntityType]:
        if self.sharding_key not in kwargs:
            msg = f'{self.sharding_key} not in args'
            raise RuntimeError(msg)
        sharding_value = kwargs[self.sharding_key]
        dao = self.mysql_dao_list[self.do_sharding(sharding_value)]
        return dao.get(**kwargs)

    def get_many(self, **kwargs) -> Generator[EntityType, None, None]:
        if self.sharding_key not in kwargs:
            msg = f'{self.sharding_key} not in args'
            raise RuntimeError(msg)
        sharding_value = kwargs[self.sharding_key]
        dao = self.mysql_dao_list[self.do_sharding(sharding_value)]
        return dao.get_many(**kwargs)

    # only valid for sharding_key
    def get_many_in(self, values: List) -> Generator[EntityType, None, None]:
        if len(values) > 0:
            dao = self.mysql_dao_list[self.do_sharding(values[0])]
            return dao.get_many_in(field=self.sharding_key, values=values)

    def save(self, item: EntityType) -> Optional[dict]:
        item_dic = item.to_dict() if isinstance(item, BaseModel) else item
        if self.sharding_key not in item_dic:
            raise RuntimeError('sharding_key not in item')
        dao = self.mysql_dao_list[self.do_sharding(item_dic[self.sharding_key])]
        return dao.save(item)

    # 读取分库分表数据，并批量返回
    def sharding_scan(
            self,
            offsets_cache: RedisHash,  # 偏移量存储在redis
            start,  # offsets_cache 其次才使用start
            scan_key=None,
            infinite_wait_secs: int = 0,  # 0表示取完数立即退出，否则等待
            worker_num: int = 1,  # 读取MySQL线程数
            part_num: int = None,  # 只读取前part个分表
    ) -> Generator[List[EntityType], None, None]:
        scan_key = scan_key or self.sharding_key
        part_num = part_num or len(self.mysql_dao_list)

        with ThreadPoolExecutor(max_workers=worker_num, thread_name_prefix='sharding_scan') as executor:
            # check whether write start to cache.
            for part_id in range(part_num):
                if offsets_cache.get(f'{part_id:03d}') is None:
                    assert start is not None
                    offsets_cache.set(f'{part_id:03d}', start)

            # initialize the future_info
            futures: List[Tuple[Optional[Future], PKType, Optional[datetime]]] = []  # (future, last_offset, last_empty_ts)
            for part_id in range(part_num):
                futures.append((None, offsets_cache.get(f'{part_id:03d}'), None))

            while True:
                # logger.debug(f'{futures}')
                # check whether to start a new job.
                part_id, offset, last_empty_ts = None, None, datetime.max
                for part_id_t in range(part_num):
                    future, offset_t, last_empty_ts_t = futures[part_id_t]
                    if not future:  # 执行中的part，不做判断
                        # 从redis读取偏移量，处理外部修改情形
                        offset_t_cache = offsets_cache.get(f'{part_id_t:03d}')
                        if offset_t_cache != offset_t:
                            logger.info(f'{self.__class__.__name__} reload offset {part_id} {offset_t} -> {offset_t_cache}')
                            offset_t = offset_t_cache

                        if not last_empty_ts_t:  # 优先做 上次scan不为空的part
                            if offset is None or offset_t < offset:  # 选择offset最小的
                                part_id, offset, last_empty_ts = part_id_t, offset_t, last_empty_ts_t
                        elif infinite_wait_secs > 0 and last_empty_ts_t + timedelta(seconds=infinite_wait_secs) < datetime.now():  # 其次做 上次scan为空的part 取为空时间最早的
                            if offset is None and last_empty_ts_t < last_empty_ts:  # 没有scan不为空的任务
                                part_id, offset, last_empty_ts = part_id_t, offset_t, last_empty_ts_t

                if offset is None and infinite_wait_secs == 0 and all(f[0] is None for f in futures):
                    # 没有新的任务 且 非无限scan 且 当前任务都已结束
                    break

                # start a new job.
                if offset is not None:
                    count = self.mysql_dao_list[part_id].batch_size
                    future_next = executor.submit(self.mysql_dao_list[part_id].scan_iter, offset, scan_key, count)
                    futures[part_id] = future_next, offset, last_empty_ts
                    logger.debug(f'start a new job {part_id}')

                # check if any future done.
                for part_id_t, (future, last_offset, last_scan_empty) in enumerate(futures):
                    if future and future.done():
                        next_offset, items = future.result()
                        logger.info(f'{self.__class__.__name__} {part_id_t} {last_offset}->{next_offset} count={len(items)}')
                        yield items
                        offsets_cache.set(f'{part_id_t:03d}', next_offset)
                        futures[part_id_t] = None, next_offset, None if next_offset != last_offset else datetime.now()

                # 轮询，设置最小等待时间片
                time.sleep(1.0)
