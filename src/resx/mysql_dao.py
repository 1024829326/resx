# encoding=utf8

import time
from typing import Type, Dict, TypeVar, Optional, Generator, List, Tuple
from datetime import date, datetime
from pymysql import ProgrammingError
from resx.mysql_client import MySQLClient
from resx.base_model import BaseModel
from resx.func import mysql_equal
from resx.log import setup_logger

logger = setup_logger(name=__name__)

EntityType = TypeVar('EntityType', BaseModel, Dict)
PKType = TypeVar('PKType', int, str, datetime, date)


# MySQLDao：对应一张具体的物理表
# 需要以id作为自增字段
# 一些考虑的问题：
#   逻辑删除OR物理删除：deleted字段 逻辑删除代替物理删除，用于scan替代kafka的场景，更新OR插入前先物理删除deleted=1的数据，并沿用blocked字段值；
#   屏蔽：blocked  0/1 非业务控制数据展示， 业务中还是会按照正常数据处理；
#   历史维度OR当前维度：属于业务层次，不用考虑；
#   数据表操作全删全插入： 这是一个“不合理需求”，基于上面起到的 删除区分、屏蔽、置为历史等 均不合理
class MySQLDao(MySQLClient):
    def __init__(
            self,
            db_tb_name: str,
            batch_size: int = 2000,
            entity_class: Type[EntityType] = None,
            primary_index_fields=None,  # Tuple[List[str], List[str]]  group keys
            ignore_fields=None,  # ignore writing fields without specified.
            enable_default_db_name=False,  # 是否打开默认db_name，不打开会进行实例级别共享连接池
            dim=None,  # Dao对应的表属于某维度，维度名称使用中文
            **kwargs,
    ):
        tokens = db_tb_name.split('.')
        assert len(tokens) == 2
        self.db_name, self.tb_name = tokens  # 指定库表名称
        self.db_tb_name: str = db_tb_name  # 兼容老代码
        self.batch_size: int = batch_size  # 批量读取数据的大小
        self.entity_class: Type[EntityType] = entity_class or dict  # 实体类
        self.primary_index_fields: Tuple[List[str], List[str]] = primary_index_fields
        self.ignore_fields: List[str] = ignore_fields or []
        super().__init__(db_name=self.db_name if enable_default_db_name else None, dim=dim, **kwargs)

    def _to_entity(self, d: Optional[Dict]) -> Optional[EntityType]:
        if d is None:
            return None
        if issubclass(self.entity_class, dict):
            return d
        return self.entity_class.from_dict(d)

    def get(self, **kwargs) -> Optional[EntityType]:
        sql_where = ('where ' if kwargs else ' ') + ' and '.join(f'{k}=%({k})s' for k in kwargs.keys())
        sql = f'select * from {self.db_tb_name} {sql_where} limit 1'
        d = self.select(sql, args=kwargs)
        return self._to_entity(d)

    def get_by_id(self, _id: PKType):
        return self.get(id=_id)

    # limit should always set. default is self.batch_size
    def get_many(self, limit=None, **kwargs) -> Generator[EntityType, None, None]:
        limit = limit or self.batch_size
        sql_where = ' and '.join(f'{k} {"is" if v is None else "="} %({k})s' for k, v in kwargs.items())
        sql = f'select * from {self.db_tb_name} where {sql_where} limit %(limit)s'
        for d in self.select_many(sql, args=kwargs | {'limit': limit}):
            item = self._to_entity(d)
            if item is not None:
                yield item

    def get_many_in(self, field: str, values: List, limit=None) -> Generator[EntityType, None, None]:
        limit = limit or self.batch_size
        sql = f'select * from {self.db_tb_name} where {field} in %s limit %s'
        for d in self.select_many(sql, args=(values, limit)):
            item = self._to_entity(d)
            if item is not None:
                yield item

    def get_max_id(self) -> PKType:
        sql = f'select max(id) from {self.db_tb_name}'
        d = self.select(sql)
        return d['max(id)']

    def delete_by_id(self, _id: PKType) -> bool:
        sql = f'delete from {self.db_tb_name} where id=%s limit 1'
        affect_rows = self.execute(sql, args=(_id,))
        return affect_rows == 1

    def table_exists(self) -> bool:
        try:
            self.get_by_id(1)
        except ProgrammingError:
            return False
        return True

    # 根据配置的primary_index_fields进行定位
    # 返回变更内容，无变更返回空
    def save(self, item: EntityType) -> dict:
        if not self.primary_index_fields:
            raise RuntimeError('primary_index_fields not configured')
        primary_index_fields = self.primary_index_fields[0] + self.primary_index_fields[1]

        item_dic = item.to_dict() if isinstance(item, BaseModel) else item
        index_args = dict((f, item_dic[f]) for f in primary_index_fields)
        item_db = self.get(**index_args)  # can be None
        item_dic_db = item_db.to_dict() if isinstance(item_db, BaseModel) else (item_db or {})

        diff_data = {}
        for f, v in item_dic.items():
            if (f in self.ignore_fields or f == 'id') and f not in primary_index_fields:
                continue
            v_db = item_dic_db.get(f)
            if mysql_equal(v_db, v):
                continue
            diff_data[f] = (v_db, v)
        diff_info = '#'.join(f'{f}:{v_db}->{v}' for f, (v_db, v) in diff_data.items())
        logger.info(f'{self.__class__.__name__} insert={item_db is None} index={index_args} diff={diff_info}')

        if len(diff_data) == 0:
            return {}

        if item_db is None:  # insert 场景
            sql_sets = ', '.join(f'{f}=%s' for f in diff_data)
            sql = f'insert ignore into {self.db_tb_name} set {sql_sets}'
            row_id = self.insert(sql, args=list(v for v_db, v in diff_data.values()))
            if row_id == 0:
                logger.warning(f'insert but ignored diff_data={diff_data}')
            return diff_data if row_id > 0 else {}

        else:  # update 场景
            sql_sets = ', '.join(f'{f}=%s' for f in diff_data)
            sql_condition = ' and '.join(f'{f}=%s' for f in primary_index_fields)
            sql = f'update ignore {self.db_tb_name} set {sql_sets} where {sql_condition} limit 1'
            affect_rows = self.execute(sql, args=list(v for v_db, v in diff_data.values()) + list(item_dic[f] for f in primary_index_fields))
            if affect_rows == 0:
                logger.warning(f'update but ignored diff_data={diff_data}')
            return diff_data if affect_rows == 1 else {}

    # 根据配置的primary_index_fields进行定位， 基于save更新数据
    # 返回变更的条目数，包括删除、更新、新增的
    def save_group(self, items: List[EntityType], group_index_values: List = None) -> Tuple[int, int, int]:
        if not self.primary_index_fields:
            raise RuntimeError('primary_index_fields not configured')
        if len(items) == 0:
            if not group_index_values or len(group_index_values) != len(self.primary_index_fields[0]):
                raise RuntimeError('bad group_index_values when items=[]')
        group_dic = (items[0].to_dict() if isinstance(items[0], BaseModel) else items[0]) if len(items) > 0 else dict(zip(self.primary_index_fields[0], group_index_values))

        diff_data = {}  # key_value -> [item_db, item]
        for item in items:
            item_dic = item.to_dict() if isinstance(item, BaseModel) else item
            key_value = tuple(item_dic[f] for f in self.primary_index_fields[1])
            if key_value not in diff_data:
                diff_data[key_value] = [None, None]
            diff_data[key_value][1] = item

        group_args = dict((f, group_dic[f]) for f in self.primary_index_fields[0])
        for item_db in self.get_many(**group_args):
            item_db_dic = item_db.to_dict() if isinstance(item_db, BaseModel) else item_db
            key_value = tuple(item_db_dic[f] for f in self.primary_index_fields[1])
            if key_value not in diff_data:
                diff_data[key_value] = [None, None]
            diff_data[key_value][0] = item_db

        delete_count, insert_count, update_count = 0, 0, 0
        for key_value, (item_db, item) in diff_data.items():
            if item_db is None:  # insert
                insert_count += 1
                self.save(item)
            elif item is None:  # delete
                delete_count += 1
                self.delete_by_id(item_db.id)
            else:  # maybe update
                if self.save(item):
                    update_count += 1
        logger.info(f'{self.__class__.__name__} group_args={group_args} delete_count={delete_count} insert_count={insert_count} update_count={update_count}')
        return delete_count, insert_count, update_count

    # 不包括offset位置，选取「大约」count条数据， 大约：用于保证next_offset值的数据scan完整
    def scan_iter(self, offset: PKType, scan_key: str, count: int) -> Tuple[PKType, List[EntityType]]:
        sql = f'select * from {self.db_tb_name} where {scan_key} > %s limit %s'
        next_offset, items = offset, []
        for did, d in enumerate(self.select_many(sql=sql, args=(offset, int(count*1.2)))):
            if did >= count and d[scan_key] != next_offset:
                break
            next_offset = d[scan_key]
            item = self._to_entity(d)
            if item is not None:
                items.append(item)
        return next_offset, items

    # 根据索引循环遍历数据， 基于scan_iter
    def scan(self, start, scan_key='id', total=0, infinite_sleep_secs: float = 0) -> Generator[List[EntityType], None, None]:
        """
        :param start: 索引字段开始值
        :param scan_key: 遍历的索引字段
        :param total: 总共读取行数
        :param infinite_sleep_secs: 无限循环读取表增量数据时，每次循环等候时间
        :return: 迭代器，每次返回一个约batch_size大小的数据
        """
        if infinite_sleep_secs < 0 or infinite_sleep_secs > 60:
            logger.warning('infinite_sleep_secs should be in [0, 60]')
        count, offset = 0, start
        while True:
            next_offset, items = self.scan_iter(offset=offset, scan_key=scan_key, count=self.batch_size)
            yield items
            count += len(items)
            logger.info(f'{self.db_tb_name} offset {offset}->{next_offset}')

            if infinite_sleep_secs > 0 and next_offset == offset:
                logger.info(f'{self.db_tb_name} sleep {infinite_sleep_secs} for next scan')
                time.sleep(infinite_sleep_secs)
                continue
            if offset == next_offset or 0 < total <= count:
                break
            offset = next_offset
