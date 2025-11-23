import os
import sys
from concurrent import futures

import grpc

# 处理 gRPC 生成文件路径（和 db_service 一样的套路）
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GRPC_DIR = os.path.join(BASE_DIR, "grpc_generated")
if GRPC_DIR not in sys.path:
    sys.path.append(GRPC_DIR)

import logging_pb2
import logging_pb2_grpc

from kafka_producer import KafkaLogger


class LoggingService(logging_pb2_grpc.LoggingServiceServicer):
    def __init__(self):
        self.kafka_logger = KafkaLogger()

    def PushLog(self, request_iterator, context):
        """
        Client-side streaming:
        - request_iterator: an iterator of LogMessage
        - we loop through all messages, send them to Kafka
        """
        count = 0
        for log_msg in request_iterator:
            # 简单地拼一个 text 发送到 Kafka
            text = f"[{log_msg.level}] [{log_msg.service_name}] {log_msg.timestamp} - {log_msg.message}"
            print(f"Received log: {text}")
            self.kafka_logger.send_log(key=log_msg.service_name, value=text)
            count += 1

        # flush producer at the end
        self.kafka_logger.flush()

        return logging_pb2.PushLogStatus(success=True, count=count)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingService(), server)

    port = os.getenv("LOGGING_SERVICE_PORT", "50052")
    server.add_insecure_port(f"[::]:{port}")
    print(f"Logging Service is running on port {port}...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
