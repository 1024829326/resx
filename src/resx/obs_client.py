# -*- coding: utf-8 -*-
import os
from typing import List, Generator, Tuple
import pytz
from dateutil.parser import parse
from datetime import datetime
from obs import ObsClient
from obs.client import GetResult
from resx.log import setup_logger

logger = setup_logger(name=__name__)


class OBSClient(object):
    """connect to huawei obs

    凭证读取优先级: 显式传入 ak/sk > 环境变量 RESX_OBS_AK / RESX_OBS_SK
    """

    def __init__(
            self,
            bucket_name: str = 'your-bucket-name',
            access_key_id: str = None,
            secret_access_key: str = None,
            server: str = None,
    ):
        access_key_id = access_key_id or os.environ.get('RESX_OBS_AK', '')
        secret_access_key = secret_access_key or os.environ.get('RESX_OBS_SK', '')
        server = server or os.environ.get('RESX_OBS_SERVER', '')
        self.obs_client = ObsClient(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            server=server,
        )
        self.bucket_name = bucket_name

    def __del__(self):
        self.obs_client.close()

    def get(self, path):
        obj = self.obs_client.getObject(self.bucket_name, path)
        data = obj.body.response.read().decode('utf8')
        return data

    def get_content(self, path, with_ts=False):
        obj = self.obs_client.getObject(self.bucket_name, path)
        if obj.status == 404:
            return None
        data = obj.body.response.read()
        if not with_ts:
            return data
        ts = parse(obj.body.lastModified).astimezone(pytz.timezone('Asia/Chongqing'))
        return data, ts

    def exists_file(self, f):
        b = self.obs_client.getObjectMetadata(self.bucket_name, f)
        if b.status == 404:
            return False
        return True

    def put(self, path, data):
        result = self.obs_client.putObject(self.bucket_name, path, data)
        return result.status

    def delete(self, path, recursive=False):
        if recursive:
            files = self.list(prefix=path)
            logger.info(f'recursive delete {len(files)}')
            for file in files:
                self.obs_client.deleteObject(self.bucket_name, file)
        result = self.obs_client.deleteObject(self.bucket_name, path)
        return result.status

    def list(self, prefix, size=1024, recursive=True):
        response = self.obs_client.listObjects(self.bucket_name, prefix=prefix, max_keys=size)
        if response.status < 300:
            names: List[str] = list(x['key'] for x in response.body.contents)
            if recursive:
                return names
            set_ = set()
            for name in names:
                name2 = '/'.join(name.split('/')[:-1])
                set_.add(name2)
            return list(set_)
        raise RuntimeError('obs获取对象失败 code:{} message:{}'.format(response.errorCode, response.errorMessage))

    def list2(self, prefix: str) -> Generator[Tuple[str, datetime], None, None]:
        marker = None
        while True:
            response: GetResult = self.obs_client.listObjects(self.bucket_name, prefix=prefix, marker=marker)
            if response.status < 300:
                for x in response.body.contents:
                    yield x['key'], parse(x['lastModified'])
            if response.body.is_truncated:
                marker = response.body.next_marker
            else:
                break