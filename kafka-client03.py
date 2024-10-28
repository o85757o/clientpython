from pykafka import KafkaClient
from pykafka.balancedconsumer import BalancedConsumer
from pykafka.exceptions import KafkaException
import datetime
import os
import re
import logging

class KafkaClientBase:
    def __init__(self, hosts, topic):
        self.hosts = hosts
        self.topic_name = topic
        self.client = None
        self.topic = None
        self.connect()

    def connect(self):
        try:
            self.client = KafkaClient(hosts=self.hosts)
            self.topic = self.client.topics[self.topic_name]
        except KafkaException as e:
            logging.error(f"Failed to connect to Kafka: {e}")
            raise

    def get_balanced_consumer(self, consumer_group):
        return BalancedConsumer(
            self.topic,
            consumer_group=consumer_group,
            auto_commit_enable=True#,
#            zookeeper_connect=self.client.zookeeper_hosts
        )

    def produce_message(self, message):
        with self.topic.get_sync_producer() as producer:
            producer.produce(message.encode('utf-8'))

class KafkaTimeRangeExtractor(KafkaClientBase):
    def __init__(self, hosts, topic):
        super().__init__(hosts, topic)

    def extract_by_time_range(self, days, output_dir):
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(days=days)

        consumer = self.get_balanced_consumer(b'time_range_extractor')

        file_counter = 1
        current_file = None
        current_file_size = 0

        for message in consumer:
            message_time = datetime.datetime.fromtimestamp(message.timestamp / 1000)
            if start_time <= message_time <= end_time:
                if current_file is None or current_file_size >= 20 * 1024 * 1024:  # 20MB
                    if current_file:
                        current_file.close()
                    file_name = f"{output_dir}/{message_time.strftime('%Y%m%d')}_{file_counter}.log"
                    current_file = open(file_name, 'w')
                    current_file_size = 0
                    file_counter += 1

                current_file.write(message.value.decode('utf-8') + '\n')
                current_file_size += len(message.value) + 1

        if current_file:
            current_file.close()

class KafkaKeywordExtractor(KafkaClientBase):
    def __init__(self, hosts, topic):
        super().__init__(hosts, topic)

    def extract_by_keyword(self, keyword, start_time, end_time, output_dir):
        consumer = self.get_balanced_consumer(b'keyword_extractor')

        file_counter = 1
        current_file = None
        current_file_size = 0

        for message in consumer:
            message_time = datetime.datetime.fromtimestamp(message.timestamp / 1000)
            if start_time <= message_time <= end_time:
                message_text = message.value.decode('utf-8')
                if re.search(keyword, message_text):
                    if current_file is None or current_file_size >= 20 * 1024 * 1024:  # 20MB
                        if current_file:
                            current_file.close()
                        file_name = f"{output_dir}/{message_time.strftime('%Y%m%d')}_{file_counter}.log"
                        current_file = open(file_name, 'w')
                        current_file_size = 0
                        file_counter += 1

                    current_file.write(message_text + '\n')
                    current_file_size += len(message_text) + 1

        if current_file:
            current_file.close()

# 使用示例
if __name__ == "__main__":
    kafka_hosts = "192.168.1.16:9092,192.168.1.16:9093,192.168.1.16:9094"  # Kafka集群节点
    topic = "my_topic"
    output_dir = "output_logs"

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 提取最近3天的数据
    time_extractor = KafkaTimeRangeExtractor(kafka_hosts, topic)

    time_extractor.produce_message("hello world01")

    time_extractor.extract_by_time_range(3, output_dir)

    # 根据关键字提取指定时间范围的数据
    keyword_extractor = KafkaKeywordExtractor(kafka_hosts, topic)
    start_time = datetime.datetime.now() - datetime.timedelta(days=7)
    end_time = datetime.datetime.now()
    keyword_extractor.extract_by_keyword("error", start_time, end_time, output_dir)