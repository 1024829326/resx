# resx

数据采集 / 爬虫场景的 Python 基础设施工具库，封装 MySQL、Redis、Kafka、对象存储、HTTP 请求、飞书通知、多线程任务调度等常用能力。

所有连接配置均通过 **环境变量** 或 **业务方显式传入字典** 注入，不内置任何密码、密钥或内网地址。

## 目录结构

```
resx/
├── base_model.py         # Pydantic 数据模型基类
├── config.py             # 连接配置工厂函数 + 环境变量示例
├── func.py               # 通用工具（环境判断、时间转换、IP 获取）
├── log.py                # 日志门面（loguru / 自定义 logger）
├── tunnel.py             # SSH 隧道自动穿透
├── mysql_client.py       # MySQL 连接池（PersistentDB / PooledDB）
├── mysql_dao.py          # 表级别 DAO（CRUD / 差异 save / scan）
├── redis_types.py        # Redis 封装（Hash / Queue / Register）
├── kafka_client.py       # Kafka Producer / Consumer
├── req.py                # HTTP 请求管理器（代理 / 重试 / 模板化 URL）
├── feishu.py             # 飞书 Webhook + Bot API
├── multi_task.py         # 多线程爬虫任务调度框架
├── tools.py              # BoundedExecutor（有界线程池）
├── obs_client.py         # 对象存储客户端（OBS / S3 兼容）
└── ext/
    ├── mysql_rw_splitting.py   # 读写分离 DAO
    └── mysql_sharding_dao.py   # 分库分表 DAO
```

## 核心模块

| 模块 | 关键类/函数 | 说明 |
|------|------------|------|
| `mysql_client` | `MySQLClient` | 全局连接池缓存，自动 SSH 隧道，慢查询日志 |
| `mysql_dao` | `MySQLDao` | `save()` 差异更新，`save_group()` 集合同步，`scan()` 流式遍历 |
| `redis_types` | `RedisQueue` | 基于 ZSet 的优先级 + 延迟队列 |
| `kafka_client` | `KafkaProducerClient` / `KafkaConsumerClient` | Kafka 收发封装 |
| `req` | `ReqManager` | 每线程独立 Session，URL 模板化，超时重试 |
| `multi_task` | `MultiTask` | 爬虫主循环模板，继承实现 `get_task()` + `process()` |
| `tunnel` | `TunnelMixin` | 本地开发自动建立 SSH 隧道 |
| `ext/mysql_rw_splitting` | `MySQLRWSplittingDao` | 读写分离封装，只读库 / 读写库分配置 |
| `ext/mysql_sharding_dao` | `MySQLShardingDao` | 抽象分库分表基类，多线程并发扫描 + Redis 偏移量 |

## 快速上手

### MySQL DAO

```python
from resx.mysql_dao import MySQLDao

mysql_cfg = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': os.environ['MYSQL_PASSWORD'],
}

dao = MySQLDao(
    db_tb_name='my_db.my_table',
    entity_class=MyModel,  # 继承 resx.base_model.BaseModel
    primary_index_fields=(['group_id'], ['item_id']),
    **mysql_cfg,
)

item = dao.get(id=123)
dao.save(new_item)           # 差异更新（对比后才写入）
dao.save_group(items)        # 集合同步（增/删/改）

for batch in dao.scan(start=0, total=10000):
    process(batch)           # 基于自增 ID 流式遍历
```

### 读写分离

```python
from resx.ext.mysql_rw_splitting import MySQLRWSplittingDao

dao = MySQLRWSplittingDao(
    mysql_ro={'host': 'ro-host', 'user': 'reader', ...},
    mysql_rw={'host': 'rw-host', 'user': 'writer', ...},
    db_tb_name='my_db.table',
    entity_class=MyModel,
)

dao.get(id=1)   # 走 RO
dao.save(item)  # 走 RW
```

### Redis 延迟队列

```python
from resx.redis_types import RedisQueue

redis_cfg = {
    'host': '127.0.0.1',
    'port': 6379,
    'password': None,
}

q = RedisQueue(name='task_queue', delay_secs=60, **redis_cfg)

q.push('task_001', realtime=True)  # 立即消费（优先级队列）
q.push('task_002')                  # 普通队列
q.push('task_003')                  # 60 秒后才可被消费

for task in q.generate():
    print(task)
```

### 多线程爬虫

```python
from resx.multi_task import MultiTask, TaskContext

class MyCrawler(MultiTask):
    def __init__(self):
        super().__init__(worker_num=8, task_key='url', filter_task_seen=True)

    def get_task(self):
        for url in url_queue:
            yield {'url': url}

    def process(self, tc: TaskContext):
        resp = http_client.get(tc.item['url'])
        save_to_db(resp)

MyCrawler().run()
```

### 飞书告警

```python
import resx.feishu as feishu

feishu.open_id_dict['张三'] = 'ou_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

feishu.send_feishu_message(
    ats=['张三'],
    text='任务完成，共处理 1000 条数据',
    url='https://open.feishu.cn/open-apis/bot/v2/hook/your-webhook',
)

# 使用 Bot API 发送富文本（需环境变量 FEISHU_APP_ID / FEISHU_APP_SECRET）
feishu.send_feishu_message_attachments(
    chat_id='oc_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    text='日报已生成',
    file=open('report.pdf', 'rb'),
    file_name='report.pdf',
)
```

## 环境变量

| 变量 | 用途 | 默认 |
|------|------|------|
| `RESX_MYSQL_HOST` / `RESX_MYSQL_USER` / `RESX_MYSQL_PASSWORD` | MySQL 示例配置 | `127.0.0.1` / `root` / `""` |
| `RESX_REDIS_HOST` / `RESX_REDIS_PASSWORD` | Redis 示例配置 | `127.0.0.1` / `None` |
| `RESX_KAFKA_BOOTSTRAP` | Kafka broker | `localhost:9092` |
| `RESX_SSH_HOST` / `RESX_SSH_PORT` / `RESX_SSH_USER` | SSH 隧道 | `127.0.0.1:22` |
| `RESX_OBS_AK` / `RESX_OBS_SK` / `RESX_OBS_SERVER` | 对象存储凭证 | 空（需显式传入） |
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` | 飞书 Bot API | 空 |
| `HTTP_PROXY` / `HTTPS_PROXY` | HTTP 代理 | 空 |
| `POD_ENV` | 运行环境：`online` / `yufa` / `dev` / `test` | 未设置 → 本地模式（自动建隧道 + loguru） |

## 依赖

```
pydantic                 # 数据模型
pymysql                  # MySQL 驱动
DBUtils                  # PersistentDB / PooledDB 连接池
redis                    # Redis 客户端
kafka-python             # Kafka 客户端
requests                 # HTTP
requests-toolbelt        # Multipart 上传
sshtunnel                # SSH 隧道
esdk-obs-python          # 对象存储（可选，未使用 OBS 可移除）
loguru                   # 日志（本地开发默认后端）
netifaces                # 获取本机 IP
python-dateutil         # 时间解析
pytz                     # 时区处理
```

### 可选依赖

| 包 | 说明 |
|----|------|
| `crawler_log` | 内部统一日志，仅限线上 K8s 环境使用；缺失时 `log.py` 自动 fallback 到 `loguru` |
| `paramiko` | `sshtunnel` 的传递依赖，SSH 隧道需要，一般随 `sshtunnel` 自动安装 |
| `urllib3` | `requests` 的传递依赖 |