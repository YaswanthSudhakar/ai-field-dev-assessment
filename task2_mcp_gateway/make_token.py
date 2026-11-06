"""Makes a test JWT. Usage: python make_token.py admin alice"""

import os
import sys
import time

import jwt


def make_token(role: str, sub: str = "demo-user", ttl_s: int = 3600, secret: str | None = None) -> str:
    secret = secret or os.environ.get("GATEWAY_JWT_SECRET", "dev-secret-change-me-0123456789abcdef")
    now = int(time.time())
    return jwt.encode({"sub": sub, "role": role, "iat": now, "exp": now + ttl_s}, secret, algorithm="HS256")


if __name__ == "__main__":
    role = sys.argv[1] if len(sys.argv) > 1 else "viewer"
    sub = sys.argv[2] if len(sys.argv) > 2 else "demo-user"
    sys.stdout.write(make_token(role, sub) + "\n")
