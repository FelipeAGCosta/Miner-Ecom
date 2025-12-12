# lib/redis_cache.py
import os
import json
import hashlib
import time
from typing import Any, Optional

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# decode_responses=True -> strings (UTF-8) em vez de bytes
_r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def _key(prefix: str, payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{prefix}:{h}"

def cache_get(prefix: str, payload: dict) -> Optional[Any]:
    """
    Retorna o dado desserializado.
    - Se foi salvo string JSON → retorna string JSON
    - Se foi salvo dict/list → retorna dict/list
    - Se Redis cair → retorna None (degrada silenciosamente)
    """
    k = _key(prefix, payload)
    try:
        val = _r.get(k)
        if val is None:
            return None
        # Tentamos interpretar como JSON; se falhar, devolvemos a string crua
        try:
            return json.loads(val)
        except Exception:
            return val
    except Exception:
        return None  # sem quebrar app se Redis indisponível

def cache_set(prefix: str, payload: dict, data: Any, ttl_sec: int = 900) -> None:
    """
    Salva o dado serializado.
    - Se 'data' for já uma string JSON (ex.: df.to_json()), guardamos como string.
    - Para dict/list/obj → guardamos json.dumps.
    - Se Redis cair → não levanta erro.
    """
    k = _key(prefix, payload)
    try:
        if isinstance(data, str):
            _r.set(k, data, ex=ttl_sec)  # já é JSON string
        else:
            _r.set(k, json.dumps(data, ensure_ascii=False), ex=ttl_sec)
    except Exception:
        pass

def now_ms() -> int:
    return int(time.time() * 1000)
