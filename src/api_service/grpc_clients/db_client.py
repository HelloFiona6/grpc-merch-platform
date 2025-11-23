import grpc
import os
import sys

# 导入 gRPC 生成文件
# from protos_shared import db_pb2, db_pb2_grpc

sys.path.append("/app/protos_shared")

import db_pb2
import db_pb2_grpc

class DBClient:
    def __init__(self, host: str):
        channel = grpc.insecure_channel(host)
        self.product_stub = db_pb2_grpc.ProductServiceStub(channel)
        self.user_stub = db_pb2_grpc.UserServiceStub(channel)
        self.order_stub = db_pb2_grpc.OrderServiceStub(channel)

    # ========== Product ==========
    def list_products(self):
        return self.product_stub.ListProducts(db_pb2.Empty())

    def get_product(self, product_id: int):
        return self.product_stub.GetProduct(db_pb2.ById(id=product_id))

    # ========== User ==========
    def create_user(self, username: str, password_hash: str):
        return self.user_stub.CreateUser(
            db_pb2.RegisterRequest(username=username, password_hash=password_hash)
        )

    def get_user(self, user_id: int):
        return self.user_stub.GetUser(db_pb2.ById(id=user_id))

    def update_user(self, user_id: int, username: str, active: bool):
        return self.user_stub.UpdateUser(
            db_pb2.UpdateUserRequest(id=user_id, username=username, active=active)
        )

    # ========== Order ==========
    def create_order(self, user_id: int, product_id: int, quantity: int):
        return self.order_stub.CreateOrder(
            db_pb2.NewOrder(user_id=user_id, product_id=product_id, quantity=quantity)
        )

    def get_order(self, order_id: int):
        return self.order_stub.GetOrder(db_pb2.ById(id=order_id))

    def cancel_order(self, order_id: int):
        return self.order_stub.CancelOrder(db_pb2.ById(id=order_id))
