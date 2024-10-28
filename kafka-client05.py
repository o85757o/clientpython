from pykafka import KafkaClient
from pykafka.balancedconsumer import BalancedConsumer
from pykafka.exceptions import KafkaException
import re
from datetime import datetime, timedelta
import os
import logging


class KafkaClientBase:
    def __init__(self, hosts):
        self.client = KafkaClient(hosts=hosts)
        self.consumers = {}

    def get_balanced_consumer(self, topic_name, consumer_group, num_partitions=None):
        topic = self.client.topics[topic_name]
        if num_partitions is None:
            num_partitions = len(topic.partitions)

        return BalancedConsumer(
            topic,
            consumer_group=consumer_group,
            auto_commit_enable=True,
            zookeeper_connect=None,  # 使用 KRaft 模式，不使用 ZooKeeper
            num_consumer_fetchers=num_partitions
        )

    def produce_message(self, topic_name, message):
        topic = self.client.topics[topic_name]
        with topic.get_sync_producer() as producer:
            producer.produce(message.encode('utf-8'))

    def consume_messages(self, topic_name, consumer_group, callback):
        if topic_name not in self.consumers:
            self.consumers[topic_name] = self.get_balanced_consumer(topic_name, consumer_group)

        consumer = self.consumers[topic_name]
        for message in consumer:
            if message is not None:
                callback(message)


class KafkaClientExtended(KafkaClientBase):
    def __init__(self, hosts):
        super().__init__(hosts)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def extract_recent_data(self, topic_name, consumer_group, days, output_dir):
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)

        def process_message(message):
            message_time = datetime.fromtimestamp(message.timestamp / 1000)
            if start_time <= message_time <= end_time:
                self._write_to_log(message, output_dir)

        self.consume_messages(topic_name, consumer_group, process_message)

    def extract_data_by_regex(self, topic_name, consumer_group, days, regex_pattern, output_dir):
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        pattern = re.compile(regex_pattern)

        def process_message(message):
            message_time = datetime.fromtimestamp(message.timestamp / 1000)
            if start_time <= message_time <= end_time:
                if pattern.search(message.value.decode('utf-8')):
                    self._write_to_log(message, output_dir)

        self.consume_messages(topic_name, consumer_group, process_message)

    def _write_to_log(self, message, output_dir):
        date_str = datetime.fromtimestamp(message.timestamp / 1000).strftime('%Y-%m-%d')
        log_file = os.path.join(output_dir, f"{date_str}.log")

        if os.path.exists(log_file) and os.path.getsize(log_file) >= 20 * 1024 * 1024:  # 20MB
            i = 1
            while os.path.exists(f"{log_file}.{i}"):
                i += 1
            log_file = f"{log_file}.{i}"

        with open(log_file, 'a') as f:
            f.write(f"{message.timestamp}: {message.value.decode('utf-8')}\n")


# 使用示例
if __name__ == "__main__":
    kafka_client = KafkaClientExtended("192.168.1.16:9092,192.168.1.16:9093,192.168.1.16:9094")

    kafka_client.produce_message("my_topic","hello world02")


    # 提取最近3天的数据
    kafka_client.extract_recent_data("my_topic", "my_consumer_group", 3, "/path/to/output/dir")

    # 使用正则表达式提取最近7天的数据
    kafka_client.extract_data_by_regex("my_topic", "my_consumer_group", 7, r"error|warning", "/path/to/output/dir")