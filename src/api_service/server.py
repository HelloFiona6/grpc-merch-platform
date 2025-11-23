from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
from google.protobuf.json_format import MessageToDict
import grpc
import os

from auth.jwt_utils import create_jwt, verify_jwt
from auth.password_utils import hash_password, verify_password

from grpc_clients.db_client import DBClient
from grpc_clients.log_client import LogClient

app = FastAPI(title="SUSTech Merch Store API", version="1.0.0")

DB_GRPC_HOST = f"{os.getenv('DB_GRPC_HOST', 'db_service')}:50051"
LOG_GRPC_HOST = f"{os.getenv('LOG_GRPC_HOST', 'logging_service')}:50052"
JWT_SECRET = os.getenv("JWT_SECRET", "secret")

db_client = DBClient(DB_GRPC_HOST)
log_client = LogClient(LOG_GRPC_HOST)

def pb_to_dict(pb_obj):
    return MessageToDict(pb_obj, preserving_proto_field_name=True)

def log_event(msg: str, level="INFO"):
    message = log_client.create_message(
        service_name="api_service",
        level=level,
        message=msg,
    )
    try:
        log_client.push_logs(iter([message]))
    except:
        pass 

def get_current_user_id(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing token")

    token = authorization.split(" ")[1]
    user_id = verify_jwt(token, JWT_SECRET)

    if not user_id:
        raise HTTPException(401, "Invalid or expired token")

    return user_id

@app.get("/")
def greeting():
    log_event("Greeting called")
    return {"message": "Welcome to SUSTech Merch Store"}

@app.get("/products")
def list_products():
    res = db_client.list_products()
    products = [pb_to_dict(p) for p in res.products]
    log_event("List products")
    return products

@app.get("/products/{product_id}")
def get_product(product_id: int):
    try:
        p = db_client.get_product(product_id)
    except grpc.RpcError:
        raise HTTPException(404, "Product not found")
    return pb_to_dict(p)

class RegisterRequest(BaseModel):
    username: str
    password: str
    
@app.post("/users/register")
def register(req: RegisterRequest):
    hashed = hash_password(req.password)
    try:
        user = db_client.create_user(req.username, hashed)
    except:
        raise HTTPException(500, "Cannot create user")
    log_event(f"Registered user {req.username}")
    return {
        "id": user.id,
        "username": user.username,
        "active": user.active
    }

class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/users/login")
def login(req: LoginRequest):
    found = None
    for i in range(1, 200):
        try:
            user = db_client.get_user(i)
            if user.username == req.username:
                found = user
                break
        except:
            continue

    if not found:
        raise HTTPException(404, "User not found")

    if not verify_password(req.password, found.password_hash):
        raise HTTPException(403, "Wrong password")

    token = create_jwt(found.id, JWT_SECRET)
    log_event(f"User {req.username} logged in")
    return {"token": token}

@app.get("/users/{user_id}")
def get_user(user_id: int, current_user: int = Depends(get_current_user_id)):
    try:
        user = db_client.get_user(user_id)
    except:
        raise HTTPException(404, "User not found")

    log_event(f"Fetched user {user_id}")
    return {
        "id": user.id,
        "username": user.username,
        "active": user.active
    }

class UpdateMeRequest(BaseModel):
    username: str | None = None
    active: bool | None = None

@app.put("/users/me")
def update_me(req: UpdateMeRequest, current_user: int = Depends(get_current_user_id)):

    old = db_client.get_user(current_user)
    new_username = req.username if req.username else old.username
    new_active = old.active if req.active is None else req.active

    updated = db_client.update_user(current_user, new_username, new_active)
    log_event(f"Updated user {current_user}")

    return {
        "id": updated.id,
        "username": updated.username,
        "active": updated.active
    }

@app.post("/users/{user_id}/deactivate")
def deactivate_user(user_id: int, current_user: int = Depends(get_current_user_id)):
    try:
        user = db_client.get_user(user_id)
    except:
        raise HTTPException(404, "User not found")

    updated = db_client.update_user(user_id, user.username, False)
    log_event(f"Deactivated user {user_id}")
    
    return {
        "id": updated.id,
        "username": updated.username,
        "active": updated.active
    }

class PlaceOrderRequest(BaseModel):
    product_id: int
    quantity: int

@app.post("/orders")
def place_order(req: PlaceOrderRequest, current_user: int = Depends(get_current_user_id)):

    if req.quantity <= 0 or req.quantity > 3:
        raise HTTPException(400, "Quantity must be <= 3")

    try:
        order = db_client.create_order(current_user, req.product_id, req.quantity)
    except:
        raise HTTPException(500, "Cannot create order")

    log_event(f"Order placed by user {current_user}")
    return pb_to_dict(order)

@app.post("/orders/{order_id}/cancel")
def cancel_order(order_id: int, current_user: int = Depends(get_current_user_id)):

    try:
        order = db_client.cancel_order(order_id)
    except:
        raise HTTPException(500, "Cannot cancel order")

    log_event(f"Order {order_id} canceled")
    return pb_to_dict(order)

@app.get("/orders/{order_id}")
def get_order(order_id: int, current_user: int = Depends(get_current_user_id)):
    try:
        order = db_client.get_order(order_id)
    except:
        raise HTTPException(404, "Order not found")

    log_event(f"Fetched order {order_id}")
    return pb_to_dict(order)
