# crawler_amazon_batch.py
"""
Batch de mineração Amazon-first.

- Lê as categorias/subcategorias do search_tasks.yaml
- Gira as tasks usando um arquivo de estado (crawler_state.json)
- Para cada task, chama discover_amazon_products(...)
- Salva os resultados na tabela amazon_products (via upsert_amazon_products)
- Mantém source_root_name, source_child_name e search_kw preenchidos

Pensado para rodar tanto manualmente (terminal) quanto via .bat / Agendador.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

from integrations.amazon_matching import discover_amazon_products
from lib.config import make_engine
from lib.db import upsert_amazon_products

# ---------------------------------------------------------------------------
# Constantes de caminho
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "crawler_state.json"


@dataclass
class Task:
    root_name: str
    child_name: str
    amazon_kw: str
    browse_node_id: Optional[int] = None


# ---------------------------------------------------------------------------
# Utilidades de impressão (sem emoji para não quebrar encoding do .bat)
# ---------------------------------------------------------------------------


def info(msg: str) -> None:
    print(f"[INFO] {msg}")


def warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def error(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Localização do search_tasks.yaml
# ---------------------------------------------------------------------------


def _find_search_tasks_file() -> Path:
    """
    Tenta localizar o search_tasks.yaml em alguns caminhos comuns.
    """
    candidates = [
        BASE_DIR / "search_tasks.yaml",
        BASE_DIR / "config" / "search_tasks.yaml",
        BASE_DIR / "assets" / "search_tasks.yaml",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(
        "search_tasks.yaml não encontrado. Verifique se ele está na raiz do projeto "
        "ou em config/search_tasks.yaml."
    )


# ---------------------------------------------------------------------------
# Carregamento de tasks a partir do search_tasks.yaml
# ---------------------------------------------------------------------------


def _load_tasks(root_filter: Optional[str] = None) -> List[Task]:
    """
    Lê o search_tasks.yaml e retorna uma lista achatada de Task(root, child, amazon_kw).

    Suporta vários formatos de YAML:
    - {"roots": [ {...}, {...} ]}
    - {"roots": { "slug": {...}, "slug2": {...} }}
    - [ {...}, {...} ]
    - { "qualquer_nome": {...}, "outro": {...} }  (cada value é um root)
    """
    path = _find_search_tasks_file()
    info(f"Usando arquivo de tasks: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    tasks: List[Task] = []

    # normaliza filtro
    root_filter_norm = (root_filter or "").strip().lower()

    def add_root(root: Dict[str, Any]) -> None:
        """Adiciona root + seus filhos à lista de tasks."""
        if not isinstance(root, dict):
            return

        root_name = str(root.get("root_name") or root.get("name") or "").strip()
        if not root_name:
            root_name = "-"

        # aplica filtro, se houver
        if root_filter_norm and root_filter_norm not in root_name.lower():
            return

        root_kw = str(root.get("amazon_kw") or "").strip()
        root_browse = root.get("amazon_browse_node_id") or root.get("browse_node_id")

        # Task da categoria-mãe (se tiver amazon_kw próprio)
        if root_kw:
            tasks.append(
                Task(
                    root_name=root_name,
                    child_name="-",
                    amazon_kw=root_kw,
                    browse_node_id=root_browse,
                )
            )

        # filhos / subcategorias
        children = root.get("children") or []
        if isinstance(children, dict):
            # suporta formato children: {slug: {...}, ...}
            children = list(children.values())

        for child in children:
            if not isinstance(child, dict):
                continue

            child_name = str(child.get("name") or "").strip() or "-"
            # se o filho não tiver amazon_kw próprio, herda o do root
            child_kw = str(child.get("amazon_kw") or root_kw or "").strip()
            child_browse = (
                child.get("amazon_browse_node_id")
                or child.get("browse_node_id")
                or root_browse
            )

            if not child_kw:
                # sem keyword -> não tem o que buscar
                continue

            tasks.append(
                Task(
                    root_name=root_name,
                    child_name=child_name,
                    amazon_kw=child_kw,
                    browse_node_id=child_browse,
                )
            )

    # Detecta formato e preenche as roots
    if isinstance(data, list):
        # Raiz já é uma lista de roots
        for root in data:
            if isinstance(root, dict):
                add_root(root)
    elif isinstance(data, dict):
        if "roots" in data:
            raw = data["roots"]
            if isinstance(raw, dict):
                roots_iter = list(raw.values())
            else:
                roots_iter = raw or []
            for root in roots_iter:
                if isinstance(root, dict):
                    add_root(root)
        else:
            # Pode ser um único root...
            if "root_name" in data or "children" in data or "amazon_kw" in data:
                add_root(data)
            else:
                # ...ou um dicionário onde cada value é um root
                for v in data.values():
                    if isinstance(v, dict):
                        add_root(v)

    return tasks


# ---------------------------------------------------------------------------
# Estado do crawler (para rotação das tasks)
# ---------------------------------------------------------------------------


def _load_state() -> Dict[str, Any]:
    """
    Carrega o arquivo de estado (crawler_state.json).
    Se não existir ou estiver corrompido, volta estado inicial.
    """
    if not STATE_FILE.exists():
        return {"last_task_index": -1, "last_run_at": None}
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            raise ValueError("estado inválido")
        return state
    except Exception:
        return {"last_task_index": -1, "last_run_at": None}


def _save_state(state: Dict[str, Any]) -> None:
    """
    Salva o estado em JSON, com timestamp de última execução.
    """
    state = dict(state)
    state["last_saved_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Conversão dos itens da Amazon para o formato aceito pelo upsert_amazon_products
# ---------------------------------------------------------------------------


def _build_amazon_df_for_db(
    items: List[Dict[str, Any]],
    root_name: str,
    child_name: str,
    search_kw: str,
) -> pd.DataFrame:
    """
    Recebe a lista de dicts retornada por discover_amazon_products (chaves amazon_*)
    e converte para um DataFrame com as colunas esperadas por amazon_products.
    """
    if not items:
        return pd.DataFrame()

    df_raw = pd.DataFrame(items)

    # Começa montando a coluna asin
    out = pd.DataFrame()
    if "amazon_asin" in df_raw.columns:
        out["asin"] = df_raw["amazon_asin"]
    else:
        out["asin"] = None

    # remove linhas sem ASIN
    out = out[out["asin"].notna()].copy()
    if out.empty:
        return out

    def col(dst: str, src: str, default: Any = None) -> None:
        if src in df_raw.columns:
            out[dst] = df_raw[src]
        else:
            out[dst] = default

    col("title", "amazon_title")
    col("brand", "amazon_brand")
    col("browse_node_id", "amazon_browse_node_id")
    col("browse_node_name", "amazon_browse_node_name")
    col("gtin", "gtin")
    col("gtin_type", "gtin_type")
    col("sales_rank", "amazon_sales_rank")
    col("sales_rank_category", "amazon_sales_rank_category")
    col("price", "amazon_price")
    col("currency", "amazon_currency", default="USD")
    col("is_prime", "amazon_is_prime", default=False)
    col("fulfillment_channel", "amazon_fulfillment_channel", default="")

    # Campos de origem da task (para sabermos de onde veio)
    out["source_root_name"] = root_name
    out["source_child_name"] = child_name
    out["search_kw"] = search_kw

    # marketplace_id deixamos como None aqui; lib.db preenche a partir do .env
    out["marketplace_id"] = None

    # Reordena colunas principais (o sql_safe_amazon_frame ainda garante o resto)
    cols_order = [
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
    # Garante que todas existam
    for c in cols_order:
        if c not in out.columns:
            out[c] = None

    return out[cols_order]


# ---------------------------------------------------------------------------
# Execução de um batch
# ---------------------------------------------------------------------------


def run_batch(
    max_items: int,
    max_tasks: int,
    root_filter: Optional[str] = None,
    reset_state_flag: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Executa um batch de mineração Amazon-first.

    - max_items: máximo de itens por task (ex.: 50).
    - max_tasks: quantas tasks serão executadas neste run (ex.: 20).
    - root_filter: se fornecido, filtra tasks pelo nome da categoria-mãe.
    - reset_state_flag: se True, ignora o estado anterior e começa do início.
    - dry_run: se True, não chama API nem salva no banco; só imprime quais tasks seriam rodadas.
    """
    tasks = _load_tasks(root_filter=root_filter)
    if not tasks:
        print("[BATCH] Nenhuma task encontrada (verifique seu search_tasks.yaml).")
        return

    total_tasks = len(tasks)
    max_tasks = max(1, min(max_tasks, total_tasks))
    max_items = max(1, max_items)

    print(
        f"[BATCH] Iniciando batch Amazon-first "
        f"({total_tasks} task(s) disponíveis, até {max_tasks} task(s) neste run, "
        f"máx {max_items} itens por task)...\n"
    )

    # Estado
    if reset_state_flag:
        state = {"last_task_index": -1, "last_run_at": None}
    else:
        state = _load_state()

    last_task_index = int(state.get("last_task_index", -1))

    start_index = (last_task_index + 1) % total_tasks
    end_index = (start_index + max_tasks - 1) % total_tasks

    print(
        f"[STATE] Estado atual: last_task_index={last_task_index}, total_tasks={total_tasks}. "
        f"Este run vai executar as tasks de índice {start_index} até {end_index} "
        f"(com wrap se necessário).\n"
    )

    engine = make_engine()

    current_index = start_index
    reason = "fim das tasks configuradas para este run."
    quota_exceeded = False

    for i in range(max_tasks):
        task = tasks[current_index]
        print(
            f"[{i + 1}/{max_tasks}] [TASK] Categoria: {task.root_name} | "
            f"Subcategoria: {task.child_name}"
        )
        print(f"   - amazon_kw base: '{task.amazon_kw}'")
        print(f"   - max_items para esta tarefa: {max_items}")

        if dry_run:
            info("Dry-run ativado: não será feita chamada à API nem escrita no banco.\n")
            last_task_index = current_index
            current_index = (current_index + 1) % total_tasks
            continue

        # Callback simples de progresso
        def _progress(done: int, total: int, source: str = "amazon") -> None:
            if source == "amazon":
                # imprime em uma linha só para não poluir demais
                print(f"      [amazon] {done}/{total}", end="\r", flush=True)

        # Chamada principal Amazon
        try:
            items, stats = discover_amazon_products(
                kw=task.amazon_kw,
                amazon_price_min=None,
                amazon_price_max=None,
                amazon_offer_type="any",
                min_monthly_sales_est=None,
                browse_node_id=task.browse_node_id,
                max_items=max_items,
                progress_cb=_progress,
            )
        except Exception as e:
            print()
            error(f"Falha ao descobrir produtos na Amazon: {e}")
            items = []
            stats = {
                "catalog_seen": 0,
                "with_price": 0,
                "kept": 0,
                "errors_api": 1,
                "last_error": str(e),
            }

        # quebra de linha depois do progress
        print()

        catalog_seen = int(stats.get("catalog_seen", 0))
        with_price = int(stats.get("with_price", 0))
        kept = int(stats.get("kept", len(items)))
        errors_api = int(stats.get("errors_api", 0))
        last_error = str(stats.get("last_error", "") or "")

        # Salvar no banco
        if items:
            df_db = _build_amazon_df_for_db(
                items=items,
                root_name=task.root_name,
                child_name=task.child_name,
                search_kw=task.amazon_kw,
            )
            if df_db.empty:
                warn("Nenhuma linha válida (asin vazio) para salvar no banco.")
            else:
                try:
                    n = upsert_amazon_products(engine, df_db)
                    print(f"   [OK] upsert_amazon_products OK - linhas processadas: {n}")
                except Exception as e:
                    warn(f"Falha ao salvar no banco: {e}")
        else:
            info("Nenhum item retornado para esta task (lista vazia).")

        print(
            f"   -> Resultado: kept={kept} | catalog_seen={catalog_seen} | "
            f"with_price={with_price} | errors_api={errors_api}"
        )

        if errors_api and last_error:
            info(f"Último erro de API observado: {last_error}")

        if "QuotaExceeded" in last_error or "QUOTAEXCEEDED" in last_error:
            warn(
                "A Amazon sinalizou QUOTAEXCEEDED (cota de API atingida). "
                "Encerrando este batch para não ultrapassar a cota.\n"
            )
            reason = "QUOTAEXCEEDED detectado."
            quota_exceeded = True
            last_task_index = current_index
            break

        # avança para próxima task
        last_task_index = current_index
        current_index = (current_index + 1) % total_tasks

    # Atualiza e salva estado
    state["last_task_index"] = last_task_index
    state["last_run_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _save_state(state)

    print("\n[OK] Batch finalizado.")
    print(f"[INFO] Motivo da parada: {reason}")
    if quota_exceeded:
        warn("Na próxima execução diária o batch continuará a partir da próxima task.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawler Amazon-first para popular a tabela amazon_products."
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=50,
        help="Máximo de itens por task (default: 50).",
    )
    parser.add_argument(
        "--max-tasks",
        type=int,
        default=20,
        help="Máximo de tasks por execução (default: 20).",
    )
    parser.add_argument(
        "--root-filter",
        type=str,
        default=None,
        help="Filtra as tasks pelo nome da categoria-mãe (case-insensitive).",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Ignora o estado salvo e recomeça a rotação das tasks do início.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Não chama a API nem grava no banco; apenas mostra quais tasks seriam executadas.",
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
