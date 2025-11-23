import os
import sys
import grpc
from datetime import datetime


sys.path.append("/app/protos_shared")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_SHARED = os.path.join(BASE_DIR, "..", "..", "protos_shared")
if os.path.isdir(LOCAL_SHARED) and LOCAL_SHARED not in sys.path:
    sys.path.append(LOCAL_SHARED)

import logging_pb2
import logging_pb2_grpc


class LogClient:
    def __init__(self, host: str):
        self.channel = grpc.insecure_channel(host)
        self.stub = logging_pb2_grpc.LoggingServiceStub(self.channel)

    def push_logs(self, logs):  # logs 是 LogMessage 的迭代器
        return self.stub.PushLog(logs)

    def create_message(self, service_name: str, level: str, message: str):
        return logging_pb2.LogMessage(
            service_name=service_name,
            level=level,
            message=message,
            timestamp=datetime.utcnow().isoformat()
        )
