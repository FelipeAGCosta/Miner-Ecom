import argparse
import time
from typing import Optional, Dict, Any, List, Tuple

from sqlalchemy import text

from lib.config import make_engine
from lib.amazon_spapi import get_buybox_price


def _safe_bool_or_none(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(int(v))
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "sim"):
        return True
    if s in ("0", "false", "no", "n", "nao", "não"):
        return False
    return None


def _norm_fulfillment_channel(v: Any) -> Optional[str]:
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


def _pick(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _extract_offer_flags(price_info: Optional[Dict[str, Any]]) -> Tuple[Optional[bool], Optional[str]]:
    if not isinstance(price_info, dict):
        return None, None

    raw_fc = _pick(price_info, ["fulfillment_channel", "fulfillmentChannel", "fulfillmentChannelType"])
    fc = _norm_fulfillment_channel(raw_fc)

    raw_prime = _pick(price_info, ["is_prime", "isPrime", "prime"])
    prime_opt = _safe_bool_or_none(raw_prime)

    # fallback: se não veio prime, pelo menos marca prime quando for AMAZON (ajuda o filtro)
    if prime_opt is None and fc == "AMAZON":
        prime_opt = True
    if prime_opt is None:
        prime_opt = False

    return bool(prime_opt), fc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=500, help="Quantos ASINs processar neste run")
    ap.add_argument("--sleep", type=float, default=0.15, help="Pausa entre calls no SP-API")
    ap.add_argument("--apply", action="store_true", help="Aplicar updates no DB (senão é dry-run)")
    ap.add_argument(
        "--only-missing",
        action="store_true",
        help="Atualiza só quando fulfillment_channel está NULL/vazio (recomendado)",
    )
    args = ap.parse_args()

    engine = make_engine()

    where_clause = ""
    if args.only_missing:
        where_clause = "WHERE (fulfillment_channel IS NULL OR fulfillment_channel='')"
    # Se você quiser incluir também is_prime NULL:
    # where_clause = "WHERE (fulfillment_channel IS NULL OR fulfillment_channel='' OR is_prime IS NULL)"

    q = f"""
        SELECT asin
        FROM amazon_products
        {where_clause}
        ORDER BY fetched_at DESC
        LIMIT :limit
    """

    with engine.begin() as conn:
        rows = conn.execute(text(q), {"limit": args.limit}).fetchall()

    asins = [r[0] for r in rows if r and r[0]]
    if not asins:
        print("[OK] Nada para backfill (nenhum ASIN encontrado).")
        return 0

    print(f"[INFO] ASINs para processar: {len(asins)}  | apply={args.apply}")

    updated = 0
    skipped = 0
    errors = 0

    for i, asin in enumerate(asins, start=1):
        asin = str(asin).strip()
        if not asin:
            continue

        try:
            price_info = get_buybox_price(asin)
            is_prime, fc = _extract_offer_flags(price_info)

            if fc is None:
                skipped += 1
                continue

            if args.apply:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            UPDATE amazon_products
                            SET fulfillment_channel = :fc,
                                is_prime = :is_prime
                            WHERE asin = :asin
                            """
                        ),
                        {"fc": fc, "is_prime": int(bool(is_prime)), "asin": asin},
                    )

            updated += 1

        except Exception as e:
            errors += 1
            print(f"[ERROR] {asin}: {type(e).__name__}: {e}")

        if args.sleep > 0:
            time.sleep(args.sleep)

        if i % 50 == 0:
            print(f"[PROG] {i}/{len(asins)} | updated={updated} skipped={skipped} errors={errors}")

    print(f"[DONE] updated={updated} skipped={skipped} errors={errors}")
    if not args.apply:
        print("[TIP] Rode com --apply para gravar no DB.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
