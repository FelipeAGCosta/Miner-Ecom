"""
Backfill de flags de oferta no amazon_products:
- is_prime
- fulfillment_channel  (normalizado: AMAZON=FBA, MFN=FBM)

Objetivo:
Preencher colunas antigas que ficaram NULL/vazias sem alterar fetched_at.

Uso (PowerShell, na raiz do projeto):
  .\.venv\Scripts\python.exe scripts\backfill_amazon_offer_flags.py --limit 200 --only-missing --dry-run
  .\.venv\Scripts\python.exe scripts\backfill_amazon_offer_flags.py --limit 200 --only-missing

Observação:
Rodar via "python scripts\..." faz o Python usar a pasta scripts como sys.path[0].
Este arquivo injeta a raiz do projeto no sys.path para permitir "import lib.*".
"""

import os
import sys
import time
import argparse
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text

# --- garante imports do projeto (lib/ etc.) ---
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from lib.config import make_engine
from lib.amazon_spapi import get_buybox_price


def _safe_bool_or_none(v: Any) -> Optional[bool]:
    if v is None:
        return None
    return bool(v)


def _norm_fulfillment_channel(v: Any) -> Optional[str]:
    """
    Normaliza:
      AMAZON / FBA / AFN -> "AMAZON"
      MFN / FBM / MERCHANT / SELLER -> "MFN"
    """
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


def _extract_flags(price_info: Optional[Dict[str, Any]]) -> Tuple[Optional[bool], Optional[str]]:
    if not price_info or not isinstance(price_info, dict):
        return None, None

    is_prime = _safe_bool_or_none(price_info.get("is_prime"))
    fc = _norm_fulfillment_channel(price_info.get("fulfillment_channel"))
    return is_prime, fc


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200, help="Quantidade máxima de ASINs para processar.")
    ap.add_argument("--sleep", type=float, default=0.15, help="Pausa entre calls (segundos).")
    ap.add_argument("--only-missing", action="store_true", help="Só processa registros com campos faltando.")
    ap.add_argument("--dry-run", action="store_true", help="Não escreve no DB; só mostra o que faria.")
    args = ap.parse_args()

    engine = make_engine()

    where = "1=1"
    if args.only_missing:
        where = """(
            fulfillment_channel IS NULL OR fulfillment_channel = ''
            OR is_prime IS NULL
        )"""

    sel = text(f"""
        SELECT asin
        FROM amazon_products
        WHERE {where}
        ORDER BY fetched_at DESC
        LIMIT :lim
    """)

    upd_tpl_only_missing = """
        UPDATE amazon_products
        SET
            fulfillment_channel = CASE
                WHEN (fulfillment_channel IS NULL OR fulfillment_channel = '') AND :fc IS NOT NULL THEN :fc
                ELSE fulfillment_channel
            END,
            is_prime = CASE
                WHEN is_prime IS NULL AND :prime IS NOT NULL THEN :prime
                ELSE is_prime
            END
        WHERE asin = :asin
    """

    upd_tpl_overwrite = """
        UPDATE amazon_products
        SET
            fulfillment_channel = :fc,
            is_prime = :prime
        WHERE asin = :asin
    """

    total = 0
    ok = 0
    skipped = 0
    errors = 0
    would_update = 0

    with engine.connect() as conn:
        rows = conn.execute(sel, {"lim": int(args.limit)}).mappings().all()

    if not rows:
        print("[INFO] Nada para processar (nenhum asin encontrado pelo filtro).")
        return

    total = len(rows)
    print(f"[INFO] Selecionados {total} ASINs para backfill. only_missing={bool(args.only_missing)} dry_run={bool(args.dry_run)}")

    # transação só para escrita
    conn = engine.connect()
    tx = conn.begin() if not args.dry_run else None

    try:
        for i, r in enumerate(rows, start=1):
            asin = (r.get("asin") or "").strip()
            if not asin:
                skipped += 1
                continue

            try:
                price_info = get_buybox_price(asin)
                is_prime, fc = _extract_flags(price_info)

                # se não conseguimos extrair nada útil, não adianta escrever
                if is_prime is None and fc is None:
                    skipped += 1
                    print(f"[{i}/{total}] asin={asin} -> sem flags (skip)")
                    time.sleep(float(args.sleep))
                    continue

                params = {
                    "asin": asin,
                    "prime": None if is_prime is None else int(bool(is_prime)),
                    "fc": fc,
                }

                if args.only_missing:
                    upd = text(upd_tpl_only_missing)
                else:
                    upd = text(upd_tpl_overwrite)

                if args.dry_run:
                    would_update += 1
                    print(f"[{i}/{total}] DRY asin={asin} prime={params['prime']} fc={params['fc']}")
                else:
                    conn.execute(upd, params)
                    ok += 1
                    if i % 25 == 0:
                        print(f"[{i}/{total}] OK... (prime={params['prime']} fc={params['fc']})")

            except Exception as e:
                errors += 1
                print(f"[{i}/{total}] ERROR asin={asin}: {type(e).__name__}: {e}")

            time.sleep(float(args.sleep))

        if not args.dry_run and tx is not None:
            tx.commit()

    except Exception:
        if not args.dry_run and tx is not None:
            tx.rollback()
        raise
    finally:
        conn.close()

    print("--------------------------------------------------")
    print(f"[DONE] total={total} ok={ok} dry_updates={would_update} skipped={skipped} errors={errors}")


if __name__ == "__main__":
    main()
