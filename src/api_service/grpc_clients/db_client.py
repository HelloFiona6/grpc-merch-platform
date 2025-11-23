import sys
import grpc

sys.path.append("/app/protos_shared")

import db_pb2
import db_pb2_grpc


class DBClient:
    def __init__(self, host: str):
        self.channel = grpc.insecure_channel(host)
        self.product_stub = db_pb2_grpc.ProductServiceStub(self.channel)
        self.user_stub = db_pb2_grpc.UserServiceStub(self.channel)
        self.order_stub = db_pb2_grpc.OrderServiceStub(self.channel)

    def list_products(self):
        return self.product_stub.ListProducts(db_pb2.Empty())

    def get_product(self, product_id):
        return self.product_stub.GetProduct(db_pb2.ProductId(id=product_id))

    def create_user(self, username, password_hash):
        return self.user_stub.CreateUser(
            db_pb2.CreateUserRequest(
                username=username,
                password_hash=password_hash
            )
        )

    def get_user(self, user_id):
        return self.user_stub.GetUser(db_pb2.UserId(id=user_id))

    def update_user(self, user_id, username, active):
        return self.user_stub.UpdateUser(
            db_pb2.UpdateUserRequest(
                id=user_id,
                username=username,
                active=active
            )
        )

    def create_order(self, user_id, product_id, quantity):
        return self.order_stub.CreateOrder(
            db_pb2.CreateOrderRequest(
                user_id=user_id,
                product_id=product_id,
                quantity=quantity
            )
        )

    def get_order(self, order_id):
        return self.order_stub.GetOrder(db_pb2.OrderId(id=order_id))

    def cancel_order(self, order_id):
        return self.order_stub.CancelOrder(db_pb2.OrderId(id=order_id))
