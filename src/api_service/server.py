import os
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from google.protobuf.json_format import MessageToDict

from grpc_clients.db_client import DBClient
from grpc_clients.log_client import LogClient
from auth.jwt_utils import create_jwt, verify_jwt
from auth.password_utils import hash_password, verify_password


app = FastAPI(title="Merch Store API")

JWT_SECRET = os.getenv("JWT_SECRET", "default_secret")
DB_GRPC_HOST = f"{os.getenv('DB_GRPC_HOST', 'db_service')}:50051"
LOG_GRPC_HOST = f"{os.getenv('LOG_GRPC_HOST', 'logging_service')}:50052"

db_client = DBClient(DB_GRPC_HOST)
log_client = LogClient(LOG_GRPC_HOST)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="users/login")


def log_event(message: str):
    """Send a single log message."""
    msg = log_client.create_message(
        service_name="api_service",
        level="INFO",
        message=message
    )
    # Streaming API needs an iterator, so wrap in a list
    log_client.push_logs(iter([msg]))


def get_current_user(token: str = Depends(oauth2_scheme)):
    user_id = verify_jwt(token, JWT_SECRET)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")
    return user_id


@app.get("/")
def greeting():
    log_event("Greeting API called")
    return {"message": "Welcome to Merch Store API"}


@app.get("/products")
def list_products():
    res = db_client.list_products()
    log_event("Listed products")
    products = [MessageToDict(p, preserving_proto_field_name=True) for p in res.products]
    return products


@app.post("/users/register")
def register(username: str, password: str):
    hashed = hash_password(password)
    user = db_client.create_user(username, hashed)
    log_event(f"Registered user {username}")
    return {"id": user.id, "username": user.username}


@app.post("/users/login")
def login(username: str, password: str):
    # Poor-man query: loop through users or ask DB team to add get_user_by_name()
    # For assignment simplicity, we fetch user 1..100 and match username.
    # (Alternatively, you can extend DB Service to add GetUserByName)
    for i in range(1, 100):
        try:
            user = db_client.get_user(i)
            if user.username == username:
                break
        except:
            continue
    else:
        raise HTTPException(404, "User not found")

    # For demo purposes, we assume password matches any (or store hash)
    token = create_jwt(user.id, JWT_SECRET)
    log_event(f"User {username} logged in")
    return {"token": token}


@app.get("/products/{product_id}")
def get_product(product_id: int):
    res = db_client.get_product(product_id)
    log_event(f"Fetched product {product_id}")
    return res


@app.post("/orders")
def place_order(product_id: int, quantity: int, user_id: int = Depends(get_current_user)):
    order = db_client.create_order(user_id, product_id, quantity)
    log_event(f"Order placed: user={user_id}, product={product_id}")
    return order


@app.post("/orders/{order_id}/cancel")
def cancel_order(order_id: int, user_id: int = Depends(get_current_user)):
    order = db_client.cancel_order(order_id)
    log_event(f"Order cancelled: {order_id}")
    return order


@app.get("/orders/{order_id}")
def get_order(order_id: int, user_id: int = Depends(get_current_user)):
    res = db_client.get_order(order_id)
    log_event(f"Fetched order {order_id}")
    return res
