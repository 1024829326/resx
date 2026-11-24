# encoding=utf8

from typing import List, Optional, Generator, Tuple
from resx.mysql_dao import EntityType
from resx.mysql_dao import MySQLDao, PKType
from resx.log import setup_logger

logger = setup_logger(name=__name__)


class MySQLRWSplittingDao(object):
    """读写分离 DAO

    mysql_ro: 只读库连接配置 dict，所有 select/get/scan 走 RO
    mysql_rw: 读写库连接配置 dict（可选）。不传时默认回退为 mysql_ro，
              业务方可在测试环境显式传入独立的写库以隔离线上数据
    """

    def __init__(self, mysql_ro, mysql_rw=None, **kwargs):
        self.mysql_dao_ro = MySQLDao(**mysql_ro, **kwargs)
        rw_cfg = mysql_rw if mysql_rw else mysql_ro
        self.mysql_dao_rw = MySQLDao(**rw_cfg, **kwargs)

    def select(self, *args, **kwargs):
        return self.mysql_dao_ro.select(*args, **kwargs)

    def select_many(self, *args, **kwargs):
        return self.mysql_dao_ro.select_many(*args, **kwargs)

    def execute(self, *args, **kwargs):
        return self.mysql_dao_rw.execute(*args, **kwargs)

    def insert(self, *args, **kwargs):
        return self.mysql_dao_rw.insert(*args, **kwargs)

    def get(self, **kwargs) -> Optional[EntityType]:
        return self.mysql_dao_ro.get(**kwargs)

    def get_by_id(self, _id: PKType):
        return self.get(id=_id)

    def get_many(self, **kwargs) -> Generator[EntityType, None, None]:
        return self.mysql_dao_ro.get_many(**kwargs)

    def get_many_in(self, **kwargs) -> Generator[EntityType, None, None]:
        return self.mysql_dao_ro.get_many_in(**kwargs)

    def get_max_id(self) -> PKType:
        return self.mysql_dao_ro.get_max_id()

    def delete_by_id(self, _id: PKType) -> bool:
        return self.mysql_dao_rw.delete_by_id(_id)

    def table_exists(self) -> bool:
        return self.mysql_dao_ro.table_exists()

    def save(self, item: EntityType) -> dict:
        return self.mysql_dao_rw.save(item)

    def save_group(self, items: List[EntityType], group_index_values: List = None) -> Tuple[int, int, int]:
        return self.mysql_dao_rw.save_group(items, group_index_values)

    def scan_iter(self, offset: PKType, scan_key: str, count: int) -> Tuple[PKType, List[EntityType]]:
        return self.mysql_dao_ro.scan_iter(offset, scan_key, count)

    def scan(self, start, scan_key='id', total=0, infinite_sleep_secs: int = 0) -> Generator[List[EntityType], None, None]:
        return self.mysql_dao_ro.scan(start, scan_key, total, infinite_sleep_secs)