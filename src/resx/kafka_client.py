# encoding=utf8

import re
from kafka import KafkaProducer, KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError
from resx.tunnel import TunnelMixin
from resx.log import setup_logger

logger = setup_logger(name=__name__)


class KafkaProducerClient(TunnelMixin):

    def __init__(self, bootstrap_servers: str, kafka_topic: str):
        super().__init__()
        self.kafka_topic: str = kafka_topic
        self.tunnel = False  # 公司内部KAFKA需要挂VPN连接 所以 强制设置 不建隧道
        mo = re.fullmatch(r'([^:]+):(\d+)', bootstrap_servers)
        if mo:
            self.host, self.port = mo.group(1), int(mo.group(2))
        self.mix()
        if mo:
            bootstrap_servers = '{}:{}'.format(self.host, self.port)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        logger.info(f'init kafka producer done. {bootstrap_servers} {self.kafka_topic}')

    def write(self, message: str, **kwargs) -> bool:
        key = kwargs.pop('key', None)
        try:
            self.producer.send(
                topic=self.kafka_topic,
                key=key,
                value=message.encode('utf8'),
            )
        except KafkaError as e:
            logger.warning(f'fail write message={message} e={e}')
            return False
        self.producer.flush()
        return True

    def close(self):
        self.producer.close()


class KafkaConsumerClient(TunnelMixin):
    def __init__(self, bootstrap_servers: str, kafka_topic: str, group_id='default', **kwargs):
        super().__init__()
        self.kafka_topic: str = kafka_topic
        self.tunnel = False  # 公司内部KAFKA需要挂VPN连接 所以 强制设置 不建隧道
        mo = re.fullmatch(r'([^:]+):(\d+)', bootstrap_servers)
        if mo:
            self.host, self.port = mo.group(1), int(mo.group(2))
        self.mix()
        if mo:
            bootstrap_servers = '{}:{}'.format(self.host, self.port)
        logger.info(f'init kafka consumer {bootstrap_servers} ...')

        self.group_id: str = group_id
        self.earliest_offset: bool = kwargs.pop('earliest_offset', True)
        self.auto_commit: bool = kwargs.pop('auto_commit', True)
        auto_offset_reset = 'smallest' if self.earliest_offset else 'largest'
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=self.auto_commit,
            auto_commit_interval_ms=100,
            auto_offset_reset=auto_offset_reset,  # {'smallest': 'earliest', 'largest': 'latest'}
        )
        logger.info(f'init kafka consumer done. {self.kafka_topic}')

    def read(self, **kwargs):
        utf8_decode: bool = kwargs.pop('utf8_decode', True)
        for mid, message in enumerate(self.consumer):
            message: ConsumerRecord = message
            data = message.value
            if utf8_decode:
                try:
                    data = data.decode('utf8')
                except UnicodeDecodeError:
                    logger.warning(f'fail to decode data={data}')
                    continue
            if mid % 10000 == 0:
                logger.info(f'read {mid} partition={message.partition} offset={message.offset} {data}')
            yield data

    def close(self):
        self.consumer.close()


if __name__ == '__main__':
    consumer = KafkaConsumerClient('localhost:9092', 'test_topic', group_id='test')
    for i in consumer.read():
        print(i)
        break