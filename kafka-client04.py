from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
import datetime
import os
import re
import logging

class KafkaClientBase:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None
        self.producer = None
        self.admin_client = None

    def connect(self):
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'kafka_client_group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(config)
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})
        self.admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})

    def get_partitions(self):
        metadata = self.admin_client.list_topics(topic=self.topic)
        return metadata.topics[self.topic].partitions

    def consume_messages(self, timeout=1.0):
        self.consumer.subscribe([self.topic])
        while True:
            msg = self.consumer.poll(timeout)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue
            yield msg

    def produce_message(self, message):
        self.producer.produce(self.topic, message.encode('utf-8'))
        self.producer.flush()

class KafkaTimeRangeExtractor(KafkaClientBase):
    def __init__(self, bootstrap_servers, topic):
        super().__init__(bootstrap_servers, topic)
        self.connect()

    def extract_by_time_range(self, days, output_dir):
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(days=days)

        partitions = self.get_partitions()
        for partition in partitions:
            low, high = self.consumer.get_watermark_offsets(TopicPartition(self.topic, partition))
            self.consumer.assign([TopicPartition(self.topic, partition, low)])

            file_counter = 1
            current_file = None
            current_file_size = 0

            for message in self.consume_messages():
                message_time = datetime.datetime.fromtimestamp(message.timestamp()[1] / 1000)
                if start_time <= message_time <= end_time:
                    if current_file is None or current_file_size >= 20 * 1024 * 1024:  # 20MB
                        if current_file:
                            current_file.close()
                        file_name = f"{output_dir}/{message_time.strftime('%Y%m%d')}_{file_counter}.log"
                        current_file = open(file_name, 'w')
                        current_file_size = 0
                        file_counter += 1

                    current_file.write(message.value().decode('utf-8') + '\n')
                    current_file_size += len(message.value()) + 1

                if message.offset() + 1 >= high:
                    break

            if current_file:
                current_file.close()

class KafkaKeywordExtractor(KafkaClientBase):
    def __init__(self, bootstrap_servers, topic):
        super().__init__(bootstrap_servers, topic)
        self.connect()

    def extract_by_keyword(self, keyword, start_time, end_time, output_dir):
        partitions = self.get_partitions()
        for partition in partitions:
            low, high = self.consumer.get_watermark_offsets(TopicPartition(self.topic, partition))
            self.consumer.assign([TopicPartition(self.topic, partition, low)])

            file_counter = 1
            current_file = None
            current_file_size = 0

            for message in self.consume_messages():
                message_time = datetime.datetime.fromtimestamp(message.timestamp()[1] / 1000)
                if start_time <= message_time <= end_time:
                    message_text = message.value().decode('utf-8')
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

                if message.offset() + 1 >= high:
                    break

            if current_file:
                current_file.close()

# 使用示例
if __name__ == "__main__":
    kafka_servers = "192.168.1.16:9092,192.168.1.16:9093,192.168.1.16:9094"  # Kafka集群节点
    topic = "my_topic"
    output_dir = "output_logs"

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 提取最近3天的数据
    time_extractor = KafkaTimeRangeExtractor(kafka_servers, topic)

    time_extractor.produce_message("hello world")

    time_extractor.extract_by_time_range(3, output_dir)

    # 根据关键字提取指定时间范围的数据
    keyword_extractor = KafkaKeywordExtractor(kafka_servers, topic)
    start_time = datetime.datetime.now() - datetime.timedelta(days=7)
    end_time = datetime.datetime.now()
    keyword_extractor.extract_by_keyword("error", start_time, end_time, output_dir)