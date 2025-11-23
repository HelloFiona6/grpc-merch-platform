import time
import jwt
from typing import Optional


def create_jwt(user_id: int, secret: str, expire_seconds: int = 3600):
    payload = {
        "user_id": user_id,
        "exp": time.time() + expire_seconds
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def verify_jwt(token: str, secret: str) -> Optional[int]:
    try:
        data = jwt.decode(token, secret, algorithms=["HS256"])
        return data["user_id"]
    except Exception:
        return None
