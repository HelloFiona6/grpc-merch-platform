from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

MAX_BYTES = 72

def _sanitize(p: str) -> str:
    if p is None:
        return ""
    if not isinstance(p, str):
        p = str(p)
    b = p.encode("utf-8")
    if len(b) <= MAX_BYTES:
        return p
    return b[:MAX_BYTES].decode("utf-8", errors="ignore")

def hash_password(password: str) -> str:
    password = _sanitize(password)
    return pwd_context.hash(password)

def verify_password(password: str, hashed: str) -> bool:
    password = _sanitize(password)
    return pwd_context.verify(password, hashed)
