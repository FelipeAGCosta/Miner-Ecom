import argparse
import sys
import time
from pathlib import Path
from typing import Optional, Any, Dict, Tuple

# ✅ garante que "lib" seja importável mesmo rodando via scripts\...
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from lib.config import make_engine  # noqa: E402
from lib.amazon_spapi import get_buybox_price  # noqa: E402


def _safe_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes", "y", "sim", "s"):
            return True
        if s in ("false", "0", "no", "n", "nao", "não"):
            return False
        return None
    try:
        return bool(v)
    except Exception:
        return None


def _norm_fc(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().upper()
    if not s:
        return None
    if s in ("AMAZON", "FBA", "AFN"):
        return "AMAZON"
    if s in ("MFN", "FBM", "MERCHANT", "SELLER"):
        return "MFN"
    return s


def _extract_flags(price_info: Any) -> Tuple[Optional[bool], Optional[str]]:
    if not isinstance(price_info, dict):
        return None, None

    raw_prime = price_info.get("is_prime")
    if raw_prime is None:
        raw_prime = price_info.get("isPrime")
    if raw_prime is None:
        raw_prime = price_info.get("primeEligible")
    if raw_prime is None:
        raw_prime = price_info.get("prime")

    prime = _safe_bool(raw_prime)

    raw_fc = price_info.get("fulfillment_channel")
    if raw_fc is None:
        raw_fc = price_info.get("fulfillmentChannel")
    if raw_fc is None:
        raw_fc = price_info.get("fulfillment")
    if raw_fc is None:
        if _safe_bool(price_info.get("is_fba")) is True:
            raw_fc = "FBA"
        elif _safe_bool(price_info.get("is_fbm")) is True:
            raw_fc = "FBM"

    fc = _norm_fc(raw_fc)

    # fallback útil
    if prime is None and fc == "AMAZON":
        prime = True

    return prime, fc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200, help="quantos ASINs processar por execução")
    ap.add_argument("--sleep", type=float, default=0.10, help="pausa entre chamadas na API (segundos)")
    ap.add_argument("--only-missing", action="store_true", help="processa apenas registros com is_prime/fulfillment vazios")
    args = ap.parse_args()

    eng = make_engine()

    # seleciona candidatos
    if args.only_missing:
        sel = text("""
            SELECT amazon_asin AS asin
            FROM amazon_products
            WHERE (fulfillment_channel IS NULL OR fulfillment_channel = '')
               OR (is_prime IS NULL)
            ORDER BY fetched_at DESC
            LIMIT :lim
        """)
    else:
        sel = text("""
            SELECT amazon_asin AS asin
            FROM amazon_products
            ORDER BY fetched_at DESC
            LIMIT :lim
        """)

    rows = []
    with eng.connect() as conn:
        rows = conn.execute(sel, {"lim": int(args.limit)}).mappings().all()

    if not rows:
        print("[OK] Nada para backfill (nenhum registro encontrado com esse filtro).")
        return

    total = len(rows)
    ok = 0
    fail = 0
    updated = 0

    upd = text("""
        UPDATE amazon_products
           SET is_prime = :is_prime,
               fulfillment_channel = :fc
         WHERE amazon_asin = :asin
    """)

    with eng.begin() as conn:
        for i, r in enumerate(rows, start=1):
            asin = (r.get("asin") or "").strip()
            if not asin:
                continue

            try:
                price_info: Optional[Dict[str, Any]] = get_buybox_price(asin)
                prime_opt, fc = _extract_flags(price_info)

                # pra gravar no DB: boolean definitivo (0/1)
                is_prime = 1 if bool(prime_opt) else 0

                # se fc continua None, grava NULL (não força lixo)
                conn.execute(
                    upd,
                    {
                        "asin": asin,
                        "is_prime": is_prime,
                        "fc": fc,
                    },
                )
                ok += 1
                updated += 1
            except Exception as e:
                fail += 1
                print(f"[WARN] {i}/{total} asin={asin} erro={type(e).__name__}: {e}")

            if args.sleep > 0:
                time.sleep(float(args.sleep))

    print(f"[DONE] total={total} updated={updated} ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
