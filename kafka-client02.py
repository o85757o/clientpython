import re
import time
import os
from pykafka import KafkaClient
from datetime import datetime, timedelta


class KafkaBaseClient:
    def __init__(self, brokers):
        self.client = KafkaClient(brokers)
        self.topics = {}

    def get_topic(self, topic_name):
        if topic_name not in self.topics:
            self.topics[topic_name] = self.client.topics[topic_name]
        return self.topics[topic_name]

    def consume_messages(self, topic_name, message_handler):
        topic = self.get_topic(topic_name)
        consumer = topic.get_simple_consumer()

        for message in consumer:
            if message is not None:
                message_handler(message)

    def produce_messages(self, topic_name, messages):
        topic = self.get_topic(topic_name)
        producer = topic.get_producer()

        for message in messages:
            producer.produce(message.encode('utf-8'))


class KafkaLogExtractor(KafkaBaseClient):
    def __init__(self, brokers):
        super().__init__(brokers)

    def extract_recent_data(self, topic_name, duration_days, output_dir):
        end_time = datetime.now()
        start_time = end_time - timedelta(days=duration_days)

        log_file = self._get_log_file(output_dir, start_time.date())
        with open(log_file, 'a') as f:
            self.consume_messages(topic_name,
                                  lambda message: self._write_message_to_log(message, f, start_time, end_time))

    def extract_keyword_data(self, topic_name, duration_days, keywords, output_dir):
        end_time = datetime.now()
        start_time = end_time - timedelta(days=duration_days)
        keyword_pattern = re.compile(keywords)

        log_file = self._get_log_file(output_dir, start_time.date())
        with open(log_file, 'a') as f:
            self.consume_messages(topic_name,
                                  lambda message: self._write_matching_message_to_log(message, f, start_time, end_time,
                                                                                      keyword_pattern))

    def _write_message_to_log(self, message, file, start_time, end_time):
        message_time = self._get_message_time(message)
        if start_time <= message_time <= end_time:
            file.write(message.value.decode('utf-8') + '\n')

    def _write_matching_message_to_log(self, message, file, start_time, end_time, keyword_pattern):
        message_time = self._get_message_time(message)
        if start_time <= message_time <= end_time:
            if keyword_pattern.search(message.value.decode('utf-8')):
                file.write(message.value.decode('utf-8') + '\n')

    def _get_message_time(self, message):
        # Assuming messages have a timestamp, replace this with actual extraction logic
        return datetime.now()  # Placeholder for actual timestamp extraction

    def _get_log_file(self, output_dir, date):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        log_file = os.path.join(output_dir, f"log_{date}.txt")

        # Check file size and rotate if necessary
        if os.path.exists(log_file) and os.path.getsize(log_file) >= 20 * 1024 * 1024:
            log_file = os.path.join(output_dir, f"log_{date}_{int(time.time())}.txt")

        return log_file


# Example usage:
# brokers = "localhost:9092,localhost:9093"
# kafka_client = KafkaLogExtractor(brokers)
# kafka_client.extract_recent_data("your_topic", 3, "/path/to/logs")
# kafka_client.extract_keyword_data("your_topic", 3, r"your_regex", "/path/to/logs")


# Example usage
if __name__ == "__main__":
    brokers = "192.168.1.16:9092,192.168.1.16:9093,192.168.1.16:9094"
    kafka_client = KafkaLogExtractor(brokers)
    kafka_client.produce_messages("example_topic",  "This is a test message.")

    kafka_client.extract_recent_data("example_topic", 3, "/path/to/logs")
    kafka_client.extract_keyword_data("example_topic", 3, r"your_regex", "/path/to/logs")


