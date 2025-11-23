import os
from confluent_kafka import Producer


class KafkaLogger:
    """
    Simple wrapper around confluent-kafka Producer.
    Publishes log messages to a configured Kafka topic.
    """

    def __init__(self):
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.topic = os.getenv("KAFKA_LOG_TOPIC", "assignment2-logs")

        conf = {
            "bootstrap.servers": bootstrap_servers,
        }
        self.producer = Producer(conf)

    def _delivery_report(self, err, msg):
        if err is not None:
            # 简单打印错误，生产环境可以写到 stderr 或监控系统
            print(f"Delivery failed for record {msg.key()}: {err}")
        # else:
        #     print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def send_log(self, key: str, value: str):
        """
        Send a log message to Kafka.
        key: usually service_name or level
        value: the actual log message
        """
        self.producer.produce(
            topic=self.topic,
            key=key.encode("utf-8") if key else None,
            value=value.encode("utf-8"),
            callback=self._delivery_report,
        )
        # trigger sending
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()
