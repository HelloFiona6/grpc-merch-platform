import grpc
from concurrent import futures

from grpc_generated import db_pb2, db_pb2_grpc

from db_manager import DBManager


# 初始化数据库管理器
db = DBManager()


# -------------------------
# Implement Product Service
# -------------------------

class ProductService(db_pb2_grpc.ProductServiceServicer):
    def ListProducts(self, request, context):
        rows = db.list_products()
        products = [
            db_pb2.Product(
                id=r[0], name=r[1], category=r[2], price=float(r[3]), stock=r[4]
            )
            for r in rows
        ]
        return db_pb2.ProductList(products=products)

    def GetProduct(self, request, context):
        r = db.get_product(request.id)
        if r is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "Product not found")
        return db_pb2.Product(
            id=r[0], name=r[1], category=r[2], price=float(r[3]), stock=r[4]
        )


# -------------------------
# Implement User Service
# -------------------------

class UserService(db_pb2_grpc.UserServiceServicer):
    def CreateUser(self, request, context):
        r = db.create_user(request.username, request.password_hash)
        return db_pb2.User(id=r[0], username=r[1], active=r[2])

    def GetUser(self, request, context):
        r = db.get_user(request.id)
        if r is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "User not found")
        return db_pb2.User(id=r[0], username=r[1], active=r[2])

    def UpdateUser(self, request, context):
        r = db.update_user(request.id, request.username, request.active)
        return db_pb2.User(id=r[0], username=r[1], active=r[2])


# -------------------------
# Implement Order Service
# -------------------------

class OrderService(db_pb2_grpc.OrderServiceServicer):
    def CreateOrder(self, request, context):
        # 参数校验
        if request.quantity > 3:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Quantity cannot exceed 3")

        r = db.create_order(request.user_id, request.product_id, request.quantity)
        return db_pb2.Order(
            id=r[0], user_id=r[1], product_id=r[2],
            quantity=r[3], total_price=float(r[4]), canceled=r[5]
        )

    def GetOrder(self, request, context):
        r = db.get_order(request.id)
        if r is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "Order not found")
        return db_pb2.Order(
            id=r[0], user_id=r[1], product_id=r[2],
            quantity=r[3], total_price=float(r[4]), canceled=r[5]
        )

    def CancelOrder(self, request, context):
        r = db.cancel_order(request.id)
        return db_pb2.Order(
            id=r[0], user_id=r[1], product_id=r[2],
            quantity=r[3], total_price=float(r[4]), canceled=r[5]
        )


# -------------------------
# Start gRPC Server
# -------------------------

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    db_pb2_grpc.add_ProductServiceServicer_to_server(ProductService(), server)
    db_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    db_pb2_grpc.add_OrderServiceServicer_to_server(OrderService(), server)

    server.add_insecure_port("[::]:50051")
    print("DB Service is running on port 50051...")

    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
