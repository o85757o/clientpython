from confluent_kafka import Consumer, Producer, KafkaError
from datetime import datetime, timedelta
import os
import threading
import json

class KafkaClient:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers

    def create_consumer(self, group_id, auto_offset_reset='earliest'):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': False  # 手动提交offset
        }
        return Consumer(conf)

    def create_producer(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
        }
        return Producer(conf)


class KafkaConsumerClient(KafkaClient):
    def consume_and_log(self, topics, begin_offset, log_dir):
        consumer = self.create_consumer(group_id='consumer_group_logger')
        consumer.subscribe(topics)

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of partition for topic {msg.topic()}, partition {msg.partition()}")
                    else:
                        print(f"Error while consuming: {msg.error()}")
                    continue

                timestamp = msg.timestamp()[1] if msg.timestamp()[0] != 0 else None  # 处理时间戳
                self.log_message(msg.topic(), msg.value().decode('utf-8'), timestamp, log_dir)

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()


    def log_message(self, topic, message, timestamp, log_dir):
        current_datetime = datetime.fromtimestamp(timestamp/1000) if timestamp else datetime.now() # 使用消息时间戳或当前时间
        date_str = current_datetime.strftime("%Y%m%d")
        log_filename_prefix = os.path.join(log_dir, f"{topic}_{date_str}")

        i = 0
        while True:
            log_filename = f"{log_filename_prefix}_{i}.log"
            if not os.path.exists(log_filename) or os.path.getsize(log_filename) < 20 * 1024 * 1024:  # 20MB
                with open(log_filename, "a") as f:
                    log_entry = f"{current_datetime.isoformat()} - {message}\n"
                    f.write(log_entry)
                break
            i += 1


class KafkaProducerClient(KafkaClient):

    def produce_messages(self, topic, messages, num_iterations=1):
        producer = self.create_producer()

        for _ in range(num_iterations):
            for message in messages:
                producer.produce(topic, value=message.encode('utf-8'), callback=self.delivery_report)
                producer.poll(0)  # 触发回调函数

        producer.flush()

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


class KafkaStreamClient(KafkaClient):

    def stream_data(self, topic, output_file):
        consumer = self.create_consumer(group_id='consumer_group_streamer')
        consumer.subscribe([topic])

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                message = msg.value().decode('utf-8')
                print(message)  # 实时展示
                with open(output_file, 'a') as f:
                    f.write(message + '\n')

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()



# 示例用法
if __name__ == "__main__":
    brokers = "localhost:9092" # 请替换为你的kafka集群地址
    topics = ["test_topic1", "test_topic2"]
    log_directory = "logs"
    output_file = "stream_output.txt"

    # 消费者示例 - 从开始读取数据并记录到日志文件
    consumer_client = KafkaConsumerClient(brokers)
    consumer_client.consume_and_log(topics, "beginning", log_directory)


    # 生产者示例 - 发送消息列表
    producer_client = KafkaProducerClient(brokers)
    messages = ["message1", "message2", "message3"]
    producer_client.produce_messages("test_topic1", messages, num_iterations=2)

    # 流式处理示例
    stream_client = KafkaStreamClient(brokers)
    stream_client.stream_data("test_topic2", output_file)

