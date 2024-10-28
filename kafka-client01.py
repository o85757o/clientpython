import os
import re
import logging
from datetime import datetime, timedelta
from pykafka import KafkaClient


class KafkaBaseClient:
    def __init__(self, bootstrap_servers):
        """
        初始化 KafkaBaseClient

        :param bootstrap_servers: Kafka 集群的服务器地址列表，如 ["kafka1:9092", "kafka2:9092"]
        """
        self.bootstrap_servers = bootstrap_servers
        self.client = KafkaClient(hosts=','.join(bootstrap_servers))
        self.producer = None
        self.consumer = None

    def create_producer(self):
        """创建生产者"""
        self.producer = self.client.topics['example_topic'].get_producer()

    def create_consumer(self, topic, group_id):
        """创建消费者"""
        self.consumer = self.client.topics[topic].get_balanced_consumer(
            consumer_group=group_id,
            auto_commit_enable=True,
            reset_offset_on_empty=True
        )

    def produce(self, topic, key, value):
        """生产消息"""
        if not self.producer:
            self.create_producer()
        self.producer.produce(key.encode('utf-8'), value.encode('utf-8'))

    def consume(self, topic):
        """消费消息"""
        if not self.consumer:
            self.create_consumer(topic, 'default_group')

    def close(self):
        """关闭 Kafka 生产者和消费者"""
        if self.producer:
            self.producer.stop()
        if self.consumer:
            self.consumer.stop()


class KafkaDataExtractor(KafkaBaseClient):
    def __init__(self, bootstrap_servers, log_dir="logs"):
        """
        初始化 KafkaDataExtractor，包含日志目录

        :param bootstrap_servers: Kafka 集群的服务器地址列表
        :param log_dir: 日志文件的目录
        """
        super().__init__(bootstrap_servers)
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)

    def _configure_logging(self, topic):
        """配置日志记录"""
        logger = logging.getLogger(topic)
        logger.setLevel(logging.INFO)

        handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(self.log_dir, f"{topic}.log"),
            maxBytes=20 * 1024 * 1024,  # 20MB
            backupCount=5  # 保留 5 个备份
        )
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        logger.addHandler(handler)
        return logger

    def extract_recent_data(self, topic, days=3):
        """抽取最近 N 天的数据"""
        self.consume(topic)

        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        logger = self._configure_logging(topic)

        for message in self.consumer:
            timestamp = datetime.fromtimestamp(message.timestamp / 1000)
            if start_time <= timestamp <= end_time:
                logger.info(f"{message.key.decode('utf-8')} - {message.value.decode('utf-8')}")
            if timestamp < start_time:
                break

    def extract_data_by_regex(self, topic, keyword_regex, days=3):
        """根据关键字正则匹配抽取规定时间段的数据"""
        self.consume(topic)

        pattern = re.compile(keyword_regex)
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        logger = self._configure_logging(topic)

        for message in self.consumer:
            timestamp = datetime.fromtimestamp(message.timestamp / 1000)
            if start_time <= timestamp <= end_time and pattern.search(message.value.decode('utf-8')):
                logger.info(f"{message.key.decode('utf-8')} - {message.value.decode('utf-8')}")
            if timestamp < start_time:
                break


# 使用示例
if __name__ == "__main__":
    bootstrap_servers = ["192.168.1.16:9092", "192.168.1.16:9093", "192.168.1.16:9094"]
    extractor = KafkaDataExtractor(bootstrap_servers)

    # 生产消息
    extractor.produce("example_topic", "key1", "This is a test message.")

    # 抽取最近 3 天的数据到日志文件
    extractor.extract_recent_data("example_topic", days=3)

    # 根据关键字正则匹配抽取数据到日志文件
    extractor.extract_data_by_regex("example_topic", keyword_regex="test", days=3)

    # 关闭客户端
    extractor.close()