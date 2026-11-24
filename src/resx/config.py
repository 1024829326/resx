# encoding=utf8

import os
from typing import List, Dict


def _env_or(key: str, default=None):
    return os.environ.get(key, default)


# ---------------------------------------------------------------------------
# SSH 隧道配置：本地开发时用于穿透内网
# 设置环境变量 RESX_SSH_HOST / RESX_SSH_PORT / RESX_SSH_USER / RESX_SSH_PKEY
# 或者直接覆盖 CFG_TUNNEL
# ---------------------------------------------------------------------------
CFG_TUNNEL = {
    'ssh_address_or_host': (
        _env_or('RESX_SSH_HOST', '127.0.0.1'),
        int(_env_or('RESX_SSH_PORT', '22')),
    ),
    'ssh_username': _env_or('RESX_SSH_USER', ''),
    # 'ssh_pkey': os.path.expanduser(_env_or('RESX_SSH_PKEY_PATH', '~/.ssh/id_rsa')),
}


MYSQL_DEFAULT_PORT = 3306


def mysql_cfg(host: str, user: str, password: str, port: int = MYSQL_DEFAULT_PORT) -> Dict:
    return {'host': host, 'user': user, 'password': password, 'port': port}


# ---------------------------------------------------------------------------
# MySQL 连接配置
# 推荐通过环境变量注入，或者业务方在自己的代码中定义连接字典
# ---------------------------------------------------------------------------
CFG_MYSQL_EXAMPLE = mysql_cfg(
    host=_env_or('RESX_MYSQL_HOST', '127.0.0.1'),
    user=_env_or('RESX_MYSQL_USER', 'root'),
    password=_env_or('RESX_MYSQL_PASSWORD', ''),
)


# ---------------------------------------------------------------------------
# Redis 连接配置
# ---------------------------------------------------------------------------
REDIS_DEFAULT_PORT = 6379
CFG_REDIS_EXAMPLE = {
    'host': _env_or('RESX_REDIS_HOST', '127.0.0.1'),
    'port': REDIS_DEFAULT_PORT,
    'password': _env_or('RESX_REDIS_PASSWORD', None),
}


# ---------------------------------------------------------------------------
# Kafka broker 地址
# ---------------------------------------------------------------------------
CFG_KAFKA_EXAMPLE = _env_or('RESX_KAFKA_BOOTSTRAP', 'localhost:9092')