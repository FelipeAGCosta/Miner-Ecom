import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from lib.config import make_engine
from lib.tasks import load_categories_tree, flatten_categories
from lib.db import upsert_amazon_products
from integrations.amazon_matching import discover_amazon_products
from integrations.amazon_spapi import _load_config_from_env

# ---------------------------------------------------------------------------
# Arquivo de estado para rotação das tasks
# ---------------------------------------------------------------------------

STATE_FILE = Path(__file__).resolve().parent / "crawler_amazon_state.json"


# ---------------------------------------------------------------------------
# Carregar lista de tasks (categoria/subcategoria) a partir do YAML
# ---------------------------------------------------------------------------

def _load_tasks(root_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Carrega tasks a partir do search_tasks.yaml usando load_categories_tree + flatten_categories.

    Cada task é um dicionário com:
      - root_name: nome da categoria mãe (ex.: "Pet Shop")
      - child_name: nome da subcategoria (ex.: "Cachorros") ou "" se não houver
      - amazon_kw: keyword base para a Amazon (ex.: "dogs")
      - browse_node_id: classificationId da Amazon (se houver no YAML)
    """
    tree = load_categories_tree()
    flat = flatten_categories(tree)

    tasks: List[Dict[str, Any]] = []

    for node in flat:
        root_name = node.get("root_name") or node.get("name") or ""
        child_name = node.get("child_name") or ""
        amazon_kw = (node.get("amazon_kw") or "").strip()
        browse_node_id = node.get("amazon_browse_node_id")

        tasks.append(
            {
                "root_name": root_name,
                "child_name": child_name,
                "amazon_kw": amazon_kw,
                "browse_node_id": browse_node_id,
            }
        )

    if root_filter:
        rf = root_filter.lower()
        tasks = [t for t in tasks if rf in (t["root_name"] or "").lower()]

    return tasks


# ---------------------------------------------------------------------------
# Estado de rotação das tasks
# ---------------------------------------------------------------------------

def _load_state(total_tasks: int) -> Dict[str, Any]:
    """
    Lê o arquivo de estado. Se estiver ausente ou inválido, começa do zero.
    """
    default_state = {
        "last_task_index": -1,
        "total_tasks": total_tasks,
        "last_run_at": None,
    }

    if not STATE_FILE.exists():
        return default_state

    try:
        raw = STATE_FILE.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            return default_state
    except Exception:
        return default_state

    # Se o número de tasks mudou desde a última gravação, zera o índice
    if data.get("total_tasks") != total_tasks:
        data["last_task_index"] = -1
        data["total_tasks"] = total_tasks

    if "last_run_at" not in data:
        data["last_run_at"] = None

    return data


def _save_state(state: Dict[str, Any]) -> None:
    """
    Salva o estado em disco.
    """
    STATE_FILE.write_text(
        json.dumps(state, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Preparar DataFrame com as colunas esperadas pela tabela amazon_products
# ---------------------------------------------------------------------------

def _prepare_amazon_df(
    items: List[Dict[str, Any]],
    root_name: str,
    child_name: str,
    search_kw: str,
    marketplace_id: str,
) -> pd.DataFrame:
    """
    Converte a lista de itens retornada por discover_amazon_products no formato
    esperado pelo upsert_amazon_products / tabela amazon_products.
    """
    if not items:
        cols = [
            "asin",
            "marketplace_id",
            "title",
            "brand",
            "browse_node_id",
            "browse_node_name",
            "gtin",
            "gtin_type",
            "sales_rank",
            "sales_rank_category",
            "price",
            "currency",
            "is_prime",
            "fulfillment_channel",
            "source_root_name",
            "source_child_name",
            "search_kw",
        ]
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(items)

    out = pd.DataFrame()
    out["asin"] = df.get("amazon_asin")
    out["marketplace_id"] = marketplace_id
    out["title"] = df.get("amazon_title")
    out["brand"] = df.get("amazon_brand")
    out["browse_node_id"] = df.get("amazon_browse_node_id")
    out["browse_node_name"] = df.get("amazon_browse_node_name")
    out["gtin"] = df.get("gtin")
    out["gtin_type"] = df.get("gtin_type")
    out["sales_rank"] = df.get("amazon_sales_rank")
    out["sales_rank_category"] = df.get("amazon_sales_rank_category")
    out["price"] = df.get("amazon_price")
    out["currency"] = df.get("amazon_currency")
    out["is_prime"] = df.get("amazon_is_prime")
    out["fulfillment_channel"] = df.get("amazon_fulfillment_channel")
    out["source_root_name"] = root_name
    out["source_child_name"] = child_name or ""
    out["search_kw"] = search_kw

    # Remove linhas sem ASIN para evitar erro de NOT NULL no banco
    out = out[out["asin"].notna()].copy()

    return out


# ---------------------------------------------------------------------------
# Execução do batch
# ---------------------------------------------------------------------------

def run_batch(
    max_items: int,
    max_tasks: int,
    root_filter: Optional[str] = None,
    reset_state_flag: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Executa um batch de mineração Amazon-first, rodando até max_tasks tasks
    na sequência, respeitando o estado salvo em disco.
    """
    tasks = _load_tasks(root_filter=root_filter)
    total_tasks = len(tasks)

    if total_tasks == 0:
        print("[BATCH] Nenhuma task encontrada (verifique seu search_tasks.yaml e root_filter).")
        return

    max_tasks = max(1, min(max_tasks, total_tasks))

    print(
        f"[BATCH] Iniciando batch Amazon-first "
        f"({total_tasks} task(s) disponíveis, até {max_tasks} task(s) neste run, "
        f"máx {max_items} itens por task)...\n"
    )

    state = _load_state(total_tasks=total_tasks)

    if reset_state_flag:
        last_idx = -1
    else:
        try:
            last_idx = int(state.get("last_task_index", -1))
        except (TypeError, ValueError):
            last_idx = -1

    if last_idx >= total_tasks:
        last_idx = -1

    n_to_run = max_tasks
    start_idx = (last_idx + 1) % total_tasks
    indices: List[int] = [(start_idx + i) % total_tasks for i in range(n_to_run)]

    print(
        f"[STATE] Estado atual: last_task_index={last_idx}, total_tasks={total_tasks}. "
        f"Este run vai executar as tasks de índice {indices[0]} até {indices[-1]} "
        f"(com wrap se necessário).\n"
    )

    engine = make_engine()
    cfg = _load_config_from_env()
    marketplace_id = cfg.marketplace_id or "ATVPDKIKX0DER"

    last_idx_for_state = last_idx
    quota_exceeded = False

    def _progress(done: int, total: int, stage: str) -> None:
        if total <= 0:
            return
        if done == total or done % 10 == 0:
            print(f"      [{stage}] {done}/{total}")

    for pos, idx in enumerate(indices):
        task = tasks[idx]
        root_name = task.get("root_name") or ""
        child_name = task.get("child_name") or ""
        amazon_kw = (task.get("amazon_kw") or "").strip()
        browse_node_id = task.get("browse_node_id")

        print(
            f"[{pos + 1}/{n_to_run}] [TASK] Categoria: {root_name} | "
            f"Subcategoria: {child_name or '-'}"
        )
        print(f"   - amazon_kw base: '{amazon_kw or '(vazio)'}'")
        print(f"   - max_items para esta tarefa: {max_items}")

        am_items: List[Dict[str, Any]]
        stats: Dict[str, Any]

        try:
            am_items, stats = discover_amazon_products(
                kw=amazon_kw,
                amazon_price_min=None,
                amazon_price_max=None,
                amazon_offer_type="any",
                min_monthly_sales_est=None,
                browse_node_id=browse_node_id,
                max_items=max_items,
                progress_cb=_progress,
            )
        except Exception as e:
            print(f"   [WARN] Erro ao chamar discover_amazon_products: {e}")
            am_items = []
            stats = {
                "catalog_seen": 0,
                "with_price": 0,
                "kept": 0,
                "errors_api": 1,
                "last_error": str(e),
            }

        kept = stats.get("kept", len(am_items))
        catalog_seen = stats.get("catalog_seen", 0)
        with_price = stats.get("with_price", 0)
        errors_api = stats.get("errors_api", 0)
        last_error = stats.get("last_error", "")

        print(
            f"   -> Resultado: kept={kept} | catalog_seen={catalog_seen} | "
            f"with_price={with_price} | errors_api={errors_api}"
        )

        if errors_api and last_error:
            print(f"   [INFO] Último erro de API observado: {last_error}")

        if am_items and not dry_run:
            df = _prepare_amazon_df(
                items=am_items,
                root_name=root_name,
                child_name=child_name,
                search_kw=amazon_kw,
                marketplace_id=marketplace_id,
            )
            if df.empty:
                print("   [INFO] Lista de itens retornada, mas nenhum ASIN válido para salvar.")
            else:
                try:
                    rows = upsert_amazon_products(engine, df)
                    print(f"   [OK] upsert_amazon_products OK - linhas processadas: {rows}")
                except Exception as e:
                    print(
                        "   [WARN] Falha ao salvar no banco "
                        "(mas a mineração em si funcionou): "
                        f"{e}"
                    )
        elif not am_items:
            print("   [INFO] Nenhum item retornado para esta tarefa (lista vazia).")

        last_idx_for_state = idx

        if "QuotaExceeded" in (last_error or ""):
            print(
                "\n[WARN] A Amazon sinalizou QUOTAEXCEEDED (cota de API atingida). "
                "Encerrando este batch para não ultrapassar a cota.\n"
            )
            quota_exceeded = True
            break

        # Pequeno intervalo entre tasks (só para não bombardear nada em sequência)
        time.sleep(2.0)

    state["last_task_index"] = last_idx_for_state
    state["total_tasks"] = total_tasks
    state["last_run_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _save_state(state)

    print("\n[OK] Batch finalizado.")
    if quota_exceeded:
        print("[INFO] Motivo da parada: QUOTAEXCEEDED detectado.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawler Amazon-first: popula a tabela amazon_products a partir das tasks do search_tasks.yaml."
    )
    parser.add_argument(
        "--root-filter",
        type=str,
        default=None,
        help="Filtra tasks pelo nome da categoria raiz (case-insensitive). Ex.: --root-filter \"Pet Shop\"",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=50,
        help="Máximo de itens (ASINs distintos com preço) por task.",
    )
    parser.add_argument(
        "--max-tasks",
        type=int,
        default=20,
        help="Quantidade máxima de tasks a rodar neste batch.",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Ignora o estado salvo e recomeça a rotação das tasks do início.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Não grava no banco, apenas mostra resumos.",
    )

    args = parser.parse_args()

    run_batch(
        max_items=args.max_items,
        max_tasks=args.max_tasks,
        root_filter=args.root_filter,
        reset_state_flag=args.reset_state,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
