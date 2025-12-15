# lib/redis_cache.py
"""
Camada de cache simples em Redis, usada para tokens e respostas de APIs externas.

- Chaves são derivadas de (prefix, payload) via SHA-256 do JSON.
- Valores são armazenados como JSON (dict/list/objetos) ou string crua.
- Falhas de Redis degradam silenciosamente (retornam None / ignoram writes).
"""

import os
import json
import hashlib
import time
from typing import Any, Optional

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# decode_responses=True → strings (UTF-8) em vez de bytes
_r = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def _key(prefix: str, payload: dict) -> str:
    """
    Gera chave determinística a partir de um prefixo e de um payload (dict).

    Exemplo de formato:
        <prefix>:<hex_sha256_do_payload_json>

    O payload é serializado com sort_keys=True para garantir ordem estável.
    """
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{prefix}:{h}"


def cache_get(prefix: str, payload: dict) -> Optional[Any]:
    """
    Lê um valor do cache.

    Retorno:
      - Se chave não existir → None
      - Se valor foi salvo como JSON (dict/list/obj) → retorna o objeto desserializado
      - Se valor for uma string simples → retorna string
      - Se Redis falhar → retorna None (degrada silenciosamente)
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
        # Falha de conexão / timeout / etc. não deve derrubar o app
        return None


def cache_set(prefix: str, payload: dict, data: Any, ttl_sec: int = 900) -> None:
    """
    Salva um valor no cache.

    Comportamento:
      - Se `data` for string → salva exatamente essa string (útil p/ JSON já pronto).
      - Caso contrário → serializa via json.dumps(ensure_ascii=False).
      - TTL padrão: 900s (15 minutos).
      - Se Redis falhar → ignora silenciosamente.
    """
    k = _key(prefix, payload)
    try:
        if isinstance(data, str):
            # Já é string (possivelmente JSON), salva direto
            _r.set(k, data, ex=ttl_sec)
        else:
            _r.set(k, json.dumps(data, ensure_ascii=False), ex=ttl_sec)
    except Exception:
        # Falha de Redis não deve impactar o fluxo principal
        pass


def now_ms() -> int:
    """
    Retorna timestamp atual em milissegundos (inteiro).
    Útil para medições simples de tempo/latência.
    """
    return int(time.time() * 1000)
