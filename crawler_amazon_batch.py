import argparse
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from lib.config import make_engine
from lib.tasks import load_categories_tree
from integrations.amazon_matching import discover_amazon_products
from lib.db import upsert_amazon_products

# ---------------------------------------------------------------------------
# Arquivo de estado do crawler (para rotacionar as tasks)
# ---------------------------------------------------------------------------
STATE_PATH = Path(__file__).resolve().parent / "crawler_state.json"


# ---------------------------------------------------------------------------
# Montagem das "tasks" a partir do search_tasks.yaml
# Cada task = (root_name, child_name opcional, amazon_kw, browse_node_id)
# ---------------------------------------------------------------------------
def build_tasks(root_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Lê a árvore de categorias e monta uma lista de tasks.

    Cada task é um dicionário com:
      - root_name
      - child_name (ou None)
      - amazon_kw (keyword base para a SP-API)
      - browse_node_id (category_id, se existir)
    """
    tree = load_categories_tree()
    tasks: List[Dict[str, Any]] = []

    for root in tree:
        root_name = (root.get("name") or "").strip()
        if not root_name:
            continue

        # Se foi passado root_filter, ignoramos os outros roots
        if root_filter and root_name != root_filter:
            continue

        children = root.get("children") or []

        # Caso 1: root tem filhos -> cada filho vira uma task
        if children:
            for child in children:
                child_name = (child.get("name") or "").strip()
                if not child_name:
                    continue

                amazon_kw = (child.get("amazon_kw") or child_name or "").strip()
                if not amazon_kw:
                    amazon_kw = (root.get("amazon_kw") or root_name or "").strip()

                browse_node_id = child.get("category_id")

                tasks.append(
                    {
                        "root_name": root_name,
                        "child_name": child_name,
                        "amazon_kw": amazon_kw,
                        "browse_node_id": browse_node_id,
                    }
                )
        else:
            # Caso 2: root sem filhos -> o próprio root é a task
            amazon_kw = (root.get("amazon_kw") or root_name or "").strip()
            browse_node_id = root.get("category_id")

            tasks.append(
                {
                    "root_name": root_name,
                    "child_name": None,
                    "amazon_kw": amazon_kw,
                    "browse_node_id": browse_node_id,
                }
            )

    return tasks


# ---------------------------------------------------------------------------
# Estado: carregar / salvar
# ---------------------------------------------------------------------------
def load_state(total_tasks: int, reset: bool = False) -> Dict[str, Any]:
    """
    Carrega o estado do crawler.

    Estrutura básica:
      {
        "last_task_index": int,   # índice da última task executada
        "task_count": int,        # número de tasks na época
        "last_run_at": "2025-12-08T18:00:00Z"
      }

    Se reset=True ou se task_count mudou, zera o ponteiro (começa do início).
    """
    if reset or not STATE_PATH.exists():
        return {
            "last_task_index": -1,
            "task_count": total_tasks,
            "last_run_at": None,
        }

    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        # Se deu erro no JSON, zera o estado
        return {
            "last_task_index": -1,
            "task_count": total_tasks,
            "last_run_at": None,
        }

    if data.get("task_count") != total_tasks:
        # Mudou a quantidade de tasks (YAML alterado, etc.) -> recomeça
        data["last_task_index"] = -1
        data["task_count"] = total_tasks
        data["last_run_at"] = None

    # Garante chaves mínimas
    if "last_task_index" not in data:
        data["last_task_index"] = -1
    if "last_run_at" not in data:
        data["last_run_at"] = None

    return data


def save_state(state: Dict[str, Any]) -> None:
    """
    Salva o estado em disco (crawler_state.json).
    """
    try:
        STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        print(f"⚠️ Não foi possível salvar o estado do crawler: {e}")


# ---------------------------------------------------------------------------
# Execução de um batch com rotação automática de tasks
# ---------------------------------------------------------------------------
def run_batch(
    root_filter: Optional[str],
    max_items: int,
    max_tasks: int,
    reset_state_flag: bool,
) -> None:
    """
    Executa um batch de mineração Amazon-first.

    Se root_filter=None:
      - Usa TODAS as tasks (todas as categorias + subcategorias do YAML)
      - Aplica rotação automática via crawler_state.json
        (cada dia começa da próxima task, com wrap-around).

    Se root_filter for preenchido:
      - Considera apenas as tasks daquele root
      - NÃO usa rotação global (roda sempre a partir da primeira task filtrada)
        -> útil pra testes manuais.
    """
    tasks = build_tasks(root_filter=root_filter)
    total_tasks = len(tasks)

    if total_tasks == 0:
        if root_filter:
            print(f"⚠️ Nenhuma task encontrada para root '{root_filter}'. Verifique o search_tasks.yaml.")
        else:
            print("⚠️ Nenhuma task encontrada no search_tasks.yaml.")
        return

    print(
        f"📦 Iniciando batch Amazon-first "
        f"({total_tasks} task(s) disponíveis, até {max_tasks} task(s) neste run, "
        f"máx {max_items} itens por task)...\n"
    )

    # -------------------------------------------------------------------
    # Determinar quais índices de task serão executados neste run
    # -------------------------------------------------------------------
    if root_filter:
        # Sem rotação global quando há filtro por root
        indices = list(range(min(total_tasks, max_tasks)))
        state = None
    else:
        # Rotação global
        state = load_state(total_tasks, reset=reset_state_flag)
        last_idx = int(state.get("last_task_index", -1))

        # Próxima task depois da última
        start_idx = (last_idx + 1) % total_tasks

        indices: List[int] = []
        idx = start_idx
        while len(indices) < max_tasks and len(indices) < total_tasks:
            indices.append(idx)
            idx = (idx + 1) % total_tasks

        print(
            f"🔁 Estado atual: last_task_index={last_idx}, total_tasks={total_tasks}. "
            f"Este run vai executar as tasks de índice {indices[0]} até {indices[-1]} "
            f"(com wrap se necessário).\n"
        )

    # -------------------------------------------------------------------
    # Executar as tasks selecionadas
    # -------------------------------------------------------------------
    engine = make_engine()

    last_executed_index: Optional[int] = None

    for pos, idx in enumerate(indices, start=1):
        t = tasks[idx]
        root_name = t["root_name"]
        child_name = t["child_name"]
        amazon_kw = t["amazon_kw"]
        browse_node_id = t["browse_node_id"]

        title = f"{root_name}" + (f" | {child_name}" if child_name else "")

        print(f"[{pos}/{len(indices)}] 🔎 Categoria: {root_name} | Subcategoria: {child_name or '(sem subcategoria)'}")
        print(f"   - amazon_kw base: '{amazon_kw}'")
        print(f"   - max_items para esta tarefa: {max_items}")

        def _progress(done: int, total: int, phase: str) -> None:
            # callback simples para debug/manual (no batch geralmente não precisa de muito detalhe)
            if total <= 0:
                return
            if done == total or done % max(1, total // 5) == 0:
                print(f"      [{phase}] {done}/{total}")

        am_items, stats = discover_amazon_products(
            kw=amazon_kw,
            amazon_price_min=None,
            amazon_price_max=None,
            amazon_offer_type="any",
            min_monthly_sales_est=0,
            browse_node_id=browse_node_id,
            max_pages=None,   # usa defaults internos
            page_size=None,   # usa defaults internos
            max_items=max_items,
            progress_cb=_progress,
        )

        kept = len(am_items)
        catalog_seen = stats.get("catalog_seen", 0)
        with_price = stats.get("with_price", 0)
        errors_api = stats.get("errors_api", 0)
        last_error = stats.get("last_error") or ""

        print(
            f"   → Resultado: kept={kept} | catalog_seen={catalog_seen} | "
            f"with_price={with_price} | errors_api={errors_api}"
        )
        if last_error:
            print(f"     Último erro de API observado: {last_error}")

        # Se estourar quota, encerra o batch para não ficar batendo à toa
        if "QuotaExceeded" in last_error:
            print(
                "\n⚠️ A Amazon sinalizou QUOTAEXCEEDED (cota de API atingida). "
                "Encerrando este batch para não exceder limites.\n"
            )
            last_executed_index = idx
            break

        # Salvar no banco, se houver itens
        if kept > 0:
            df = pd.DataFrame(am_items)
            try:
                upsert_amazon_products(
                    engine,
                    df,
                    source_root_name=root_name,
                    source_child_name=child_name,
                    search_kw=amazon_kw,
                )
                print(f"   ✅ upsert_amazon_products OK - linhas processadas: {len(df)}")
            except Exception as e:
                print(
                    f"   ⚠️ Falha ao salvar no banco (mas a mineração em si funcionou): {e}"
                )
        else:
            print("   ⚠️ Nenhum item mantido nesta tarefa (nada para salvar no banco).")

        last_executed_index = idx

        # Pausa leve entre tasks (não é o delay de pricing; esse é outra camada)
        time.sleep(2.0)
        print()

    # -------------------------------------------------------------------
    # Atualizar estado global (se não havia root_filter)
    # -------------------------------------------------------------------
    if state is not None and last_executed_index is not None:
        state["last_task_index"] = int(last_executed_index)
        state["task_count"] = total_tasks
        state["last_run_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        save_state(state)

    print("\n✅ Batch finalizado.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Minerador Amazon-first em batch (com rotação de categorias/subcategorias)."
    )
    parser.add_argument(
        "--root-filter",
        type=str,
        default=None,
        help="Nome exato da categoria raiz para filtrar (ex.: 'Pet Shop'). "
             "Se omitido, roda em todas as roots do YAML.",
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
        default=10,
        help="Máximo de tasks (categoria+subcategoria) a executar neste run.",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Ignora o estado anterior e recomeça da primeira task.",
    )

    args = parser.parse_args()

    run_batch(
        root_filter=args.root_filter,
        max_items=args.max_items,
        max_tasks=args.max_tasks,
        reset_state_flag=args.reset_state,
    )


if __name__ == "__main__":
    main()
