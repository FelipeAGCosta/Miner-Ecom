# crawler_amazon_batch.py
"""
Batch de mineração Amazon-first.

- Lê as categorias/subcategorias do search_tasks.yaml
- Usa discover_amazon_products(...) para buscar produtos na Amazon
- Salva/atualiza na tabela amazon_products (via upsert_amazon_products)
- Mantém um "estado" simples em .crawler_amazon_state.json para
  continuar de onde parou entre execuções.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yaml

from lib.config import make_engine
from lib.db import upsert_amazon_products
from integrations.amazon_matching import discover_amazon_products

BASE_DIR = Path(__file__).resolve().parent
TASKS_YAML = BASE_DIR / "search_tasks.yaml"
STATE_PATH = BASE_DIR / ".crawler_amazon_state.json"


# ---------------------------------------------------------------------------
# Carregar tasks a partir do search_tasks.yaml
# ---------------------------------------------------------------------------


def _iter_root_nodes(raw: Any) -> List[Any]:
    """
    Deixa o carregamento bem tolerante ao formato do YAML.
    Tenta achar uma lista de "roots" em várias chaves comuns.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        # tenta algumas chaves comuns
        for key in ("roots", "tasks", "categories", "items"):
            val = raw.get(key)
            if isinstance(val, list):
                return val
        # fallback: considera o próprio dict como um único root
        return [raw]
    return []


def _load_tasks(root_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Lê o search_tasks.yaml e produz uma lista "achatada" de tasks:

    [
      {
        "root_name": "Pet Shop",
        "child_name": "Cachorros",
        "amazon_kw": "dogs",
        "browse_node_id": <opcional>,
      },
      ...
    ]
    """
    if not TASKS_YAML.exists():
        print(f"[BATCH] Arquivo {TASKS_YAML} não encontrado.")
        return []

    with TASKS_YAML.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    roots = _iter_root_nodes(data)
    tasks: List[Dict[str, Any]] = []

    for node in roots:
        # Root pode ser string ("Eletrônicos") ou dict
        if isinstance(node, str):
            root_name = node
            root_kw = node
            children = []
        elif isinstance(node, dict):
            root_name = (
                node.get("root_name")
                or node.get("name")
                or node.get("label")
                or ""
            )
            if not root_name:
                root_name = "Categoria"
            root_kw = (
                node.get("amazon_kw")
                or node.get("amazon_keyword")
                or node.get("kw")
            )
            children = node.get("children") or node.get("subcategories") or []
        else:
            continue

        # Filtro por nome da categoria-mãe, se solicitado
        if root_filter and root_filter.lower() not in root_name.lower():
            continue

        if children:
            # Cada filho é uma subcategoria / tarefa
            for ch in children:
                if isinstance(ch, str):
                    child_name = ch
                    child_kw: Optional[str] = root_kw or ch
                    browse_node_id = None
                elif isinstance(ch, dict):
                    child_name = (
                        ch.get("child_name")
                        or ch.get("name")
                        or ch.get("label")
                        or "-"
                    )
                    child_kw = (
                        ch.get("amazon_kw")
                        or ch.get("amazon_keyword")
                        or ch.get("kw")
                        or root_kw
                    )
                    browse_node_id = ch.get("browse_node_id")
                else:
                    continue

                if not child_kw:
                    child_kw = child_name

                tasks.append(
                    {
                        "root_name": root_name,
                        "child_name": child_name,
                        "amazon_kw": child_kw,
                        "browse_node_id": browse_node_id,
                    }
                )
        else:
            # Categoria sem filhos explícitos → vira uma task sozinha
            kw = root_kw or root_name
            tasks.append(
                {
                    "root_name": root_name,
                    "child_name": "-",
                    "amazon_kw": kw,
                    "browse_node_id": None,
                }
            )

    return tasks


# ---------------------------------------------------------------------------
# Estado simples em JSON (.crawler_amazon_state.json)
# ---------------------------------------------------------------------------


def _load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return {"last_task_index": -1}
    try:
        with STATE_PATH.open("r", encoding="utf-8") as f:
            state = json.load(f)
        if "last_task_index" not in state:
            state["last_task_index"] = -1
        return state
    except Exception:
        return {"last_task_index": -1}


def _save_state(state: Dict[str, Any]) -> None:
    try:
        with STATE_PATH.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] Falha ao salvar estado: {e}")


# ---------------------------------------------------------------------------
# Execução do batch
# ---------------------------------------------------------------------------


def run_batch(
    root_filter: Optional[str],
    max_items: int,
    max_tasks: int,
    reset_state_flag: bool,
    dry_run: bool = False,
) -> None:
    tasks = _load_tasks(root_filter=root_filter)
    if not tasks:
        print(
            "[BATCH] Nenhuma task encontrada (verifique search_tasks.yaml "
            "e o parâmetro --root-filter, se usado)."
        )
        return

    total_tasks = len(tasks)

    if reset_state_flag:
        state = {"last_task_index": -1}
    else:
        state = _load_state()

    last_task_index = int(state.get("last_task_index", -1))

    # Define quantas tasks este run vai executar
    tasks_to_run = min(max_tasks, total_tasks)
    if tasks_to_run <= 0:
        print("[BATCH] max_tasks <= 0, nada a fazer.")
        return

    # Calcula o índice inicial (com wrap)
    start_index = (last_task_index + 1) % total_tasks

    print(
        f"[BATCH] Iniciando batch Amazon-first "
        f"({total_tasks} task(s) disponíveis, até {tasks_to_run} task(s) neste run, "
        f"máx {max_items} itens por task)...\n"
    )

    print(
        f"[STATE] Estado atual: last_task_index={last_task_index}, "
        f"total_tasks={total_tasks}. Este run vai executar as tasks de "
        f"índice {start_index} até "
        f"{(start_index + tasks_to_run - 1) % total_tasks} (com wrap se necessário).\n"
    )

    engine = make_engine()
    marketplace_id = os.getenv("SPAPI_MARKETPLACE_ID", "ATVPDKIKX0DER")

    stop_reason: Optional[str] = None
    last_executed_index = last_task_index

    def _progress(done: int, total: int, source: str) -> None:
        # Apenas um print simples para acompanhar o progresso
        print(f"      [{source}] {done}/{total}")

    for offset in range(tasks_to_run):
        idx = (start_index + offset) % total_tasks
        task = tasks[idx]

        root_name = task.get("root_name") or "?"
        child_name = task.get("child_name") or "-"
        amazon_kw = (task.get("amazon_kw") or "").strip()
        browse_node_id = task.get("browse_node_id")

        print(
            f"[{offset + 1}/{tasks_to_run}] [TASK] Categoria: {root_name} "
            f"| Subcategoria: {child_name}"
        )
        print(f"   - amazon_kw base: '{amazon_kw}'")
        print(f"   - max_items para esta tarefa: {max_items}")

        if dry_run:
            print("   [DRY-RUN] Não chamando API nem salvando no banco.\n")
            last_executed_index = idx
            continue

        # Chama a descoberta Amazon-first
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
            print(f"   [ERRO] Falha ao chamar discover_amazon_products: {e}\n")
            stop_reason = "EXCEPTION"
            last_executed_index = idx
            break

        if not am_items:
            print("   -> Nenhum produto encontrado para esta tarefa.\n")
            last_executed_index = idx
            continue

        # Transforma em DataFrame e ajusta colunas para o schema do banco
        df = pd.DataFrame(am_items)

        rename_map = {
            "amazon_asin": "asin",
            "amazon_title": "title",
            "amazon_brand": "brand",
            "amazon_browse_node_id": "browse_node_id",
            "amazon_browse_node_name": "browse_node_name",
            "amazon_sales_rank": "sales_rank",
            "amazon_sales_rank_category": "sales_rank_category",
            "amazon_price": "price",
            "amazon_currency": "currency",
            "amazon_is_prime": "is_prime",
            "amazon_fulfillment_channel": "fulfillment_channel",
        }
        df = df.rename(columns=rename_map)

        # marketplace_id para todos
        df["marketplace_id"] = marketplace_id

        # >>> AQUI: adicionamos as colunas de origem da tarefa <<<
        df["source_root_name"] = root_name
        df["source_child_name"] = child_name
        df["search_kw"] = amazon_kw

        # Salva no banco
        try:
            n_rows = upsert_amazon_products(engine, df)
            print(
                f"   [OK] upsert_amazon_products OK - linhas processadas: {n_rows}"
            )
        except Exception as e:
            print(
                "   [WARN] Falha ao salvar no banco (mas a mineração em si "
                f"funcionou): {e}"
            )

        # Log de estatísticas
        kept = stats.get("kept", len(df))
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

        # Se bater QUOTAEXCEEDED, paramos o batch educadamente
        if "QuotaExceeded" in (last_error or ""):
            print(
                "\n[WARN] A Amazon sinalizou QUOTAEXCEEDED (cota de API "
                "atingida). Encerrando este batch para não ultrapassar a cota.\n"
            )
            stop_reason = "QUOTAEXCEEDED"
            last_executed_index = idx
            break

        print()  # linha em branco para separar tasks
        last_executed_index = idx

    # Atualiza estado
    state["last_task_index"] = int(last_executed_index)
    state["last_run_at"] = (
        datetime.now(timezone.utc).isoformat(timespec="seconds")
    )
    if stop_reason:
        state["last_stop_reason"] = stop_reason

    _save_state(state)

    print("[OK] Batch finalizado.")
    if stop_reason:
        print(f"[INFO] Motivo da parada: {stop_reason}.")
    else:
        print("[INFO] Motivo da parada: fim das tasks configuradas para este run.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Batch de mineração Amazon-first para popular amazon_products."
    )
    parser.add_argument(
        "--root-filter",
        type=str,
        default=None,
        help="Filtra apenas categorias-mãe cujo nome contenha este texto.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=50,
        help="Máximo de itens por task (ASINs distintos com preço).",
    )
    parser.add_argument(
        "--max-tasks",
        type=int,
        default=20,
        help="Máximo de tasks (categoria/subcategoria) por execução.",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Ignora o estado anterior e recomeça das primeiras tasks.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Não chama a API nem grava no banco (apenas mostra quais tasks rodariam).",
    )

    args = parser.parse_args()

    run_batch(
        root_filter=args.root_filter,
        max_items=args.max_items,
        max_tasks=args.max_tasks,
        reset_state_flag=args.reset_state,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
