"""
crawler_amazon_batch.py

Crawler em lote para mineração Amazon-first.

- Lê a árvore de categorias do search_tasks.yaml (via load_categories_tree).
- Gera uma lista de tarefas (root_name, child_name, amazon_kw_base).
- Para cada tarefa, chama discover_amazon_products e salva em amazon_products.
- Respeita limites simples: número máximo de tarefas por execução,
  max_items por tarefa e um sleep entre chamadas para não estourar a quota.
"""

import argparse
import time
from typing import List, Dict, Any, Iterable, Tuple, Optional

import pandas as pd

from lib.config import make_engine
from lib.tasks import load_categories_tree
from integrations.amazon_matching import discover_amazon_products
from lib.db import upsert_amazon_products


# ---------------------------------------------------------------------------
# Geração de tarefas a partir do search_tasks.yaml
# ---------------------------------------------------------------------------

def _iter_tasks_from_tree(tree: List[Dict[str, Any]]) -> Iterable[Tuple[str, Optional[str], str]]:
    """
    Gera tuplas (root_name, child_name, amazon_kw_base) a partir da
    estrutura de categorias do search_tasks.yaml.

    Regra:
      - Se tiver children: gera uma tarefa por subcategoria.
      - Se NÃO tiver children: gera tarefa só com a categoria raiz.
    """
    for root in tree:
        root_name = root.get("name")
        children = root.get("children") or []

        # preferimos usar amazon_kw se existir; senão, o próprio name
        root_kw = (root.get("amazon_kw") or root_name or "").strip()

        if children:
            for ch in children:
                child_name = ch.get("name")
                child_kw = (ch.get("amazon_kw") or child_name or root_kw).strip()
                yield root_name, child_name, child_kw
        else:
            # categoria sem filhos: ainda assim vale como uma tarefa
            yield root_name, None, root_kw


def _parse_root_filter(root_filter: Optional[str]) -> Optional[set]:
    """
    Converte "--root-filter 'Pet Shop,Casa & Cozinha'" em um set de nomes.
    Se None ou string vazia, retorna None.
    """
    if not root_filter:
        return None
    parts = [p.strip() for p in root_filter.split(",") if p.strip()]
    return set(parts) if parts else None


# ---------------------------------------------------------------------------
# Execução principal (batch)
# ---------------------------------------------------------------------------

def run_batch(
    max_items: int,
    max_tasks: int,
    sleep_seconds: float,
    root_filter: Optional[str],
    start_from: int,
) -> None:
    """
    Executa o crawler em lote:

    - max_items: máximo de itens por tarefa (por categoria/subcategoria).
    - max_tasks: máximo de tarefas (root+child) nesta execução.
    - sleep_seconds: pausa entre tarefas para aliviar a quota da API.
    - root_filter: opcional, filtra pelo nome da categoria raiz (ou várias, separadas por vírgulas).
    - start_from: índice inicial na lista de tarefas, para "continuar de onde parou".
    """
    print("🔧 Iniciando crawler em lote (Amazon-first)...")

    engine = make_engine()
    tree = load_categories_tree()

    all_tasks = list(_iter_tasks_from_tree(tree))
    print(f"📋 Total de combinações (root/subcat) encontradas no YAML: {len(all_tasks)}")

    roots_set = _parse_root_filter(root_filter)
    if roots_set:
        all_tasks = [t for t in all_tasks if t[0] in roots_set]
        print(f"📂 Após filtro de root ({', '.join(roots_set)}): {len(all_tasks)} tarefas.")

    if start_from < 0:
        start_from = 0
    if start_from >= len(all_tasks):
        print(f"⚠️ start_from={start_from} está além da lista de tarefas ({len(all_tasks)}). Nada a fazer.")
        return

    if max_tasks <= 0:
        # se max_tasks <= 0, roda tudo a partir de start_from
        tasks_slice = all_tasks[start_from:]
    else:
        tasks_slice = all_tasks[start_from:start_from + max_tasks]

    if not tasks_slice:
        print("⚠️ Nenhuma tarefa selecionada após aplicar filtros/limites.")
        return

    print(f"🚀 Rodando {len(tasks_slice)} tarefas nesta execução "
          f"(start_from={start_from}, max_tasks={max_tasks})")

    for idx, (root_name, child_name, kw_base) in enumerate(tasks_slice, start=1):
        label = child_name or "(sem subcategoria)"
        print(f"\n[{idx}/{len(tasks_slice)}] 🔎 Categoria: {root_name} | Subcategoria: {label}")
        print(f"   - amazon_kw base: '{kw_base}'")
        print(f"   - max_items para esta tarefa: {max_items}")

        try:
            items, stats = discover_amazon_products(
                kw=kw_base,
                amazon_price_min=None,
                amazon_price_max=None,
                amazon_offer_type="any",
                min_monthly_sales_est=0,
                browse_node_id=None,      # para o crawler, deixamos sem filtro de browse_node
                max_items=max_items,
                progress_cb=None,        # sem barra de progresso (modo CLI)
            )
        except Exception as e:
            print(f"   ⚠️ Erro na discover_amazon_products: {e}")
            continue

        kept = len(items)
        print(
            f"   → Resultado: kept={kept} | "
            f"catalog_seen={stats.get('catalog_seen')} | "
            f"with_price={stats.get('with_price')} | "
            f"errors_api={stats.get('errors_api')}"
        )
        last_error = stats.get("last_error")
        if last_error:
            print(f"     Último erro de API observado: {last_error}")

        if not items:
            # nada pra salvar, vai pra próxima tarefa
            continue

        df = pd.DataFrame(items)

        try:
            n = upsert_amazon_products(
                engine,
                df,
                source_root_name=root_name,
                source_child_name=child_name,
                search_kw=kw_base,
            )
            print(f"   ✅ upsert_amazon_products OK - linhas processadas: {n}")
        except Exception as e:
            print(f"   ⚠️ Falha ao salvar no banco (mas a mineração em si funcionou): {e}")

        # pequena pausa entre tarefas pra aliviar a quota da API
        if idx < len(tasks_slice) and sleep_seconds > 0:
            print(f"   ⏸ Aguardando {sleep_seconds:.1f} segundos antes da próxima tarefa...")
            time.sleep(sleep_seconds)

    print("\n🏁 Crawler em lote finalizado.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Crawler em lote Amazon-first (usa search_tasks.yaml).")
    parser.add_argument(
        "--max-items",
        type=int,
        default=100,
        help="Máximo de itens distintos por tarefa (categoria/subcategoria). Default = 100.",
    )
    parser.add_argument(
        "--max-tasks",
        type=int,
        default=5,
        help="Máximo de tarefas (root+child) nesta execução. Se 0 ou negativo, roda todas a partir de start_from.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=2.0,
        help="Pausa (em segundos) entre tarefas, para aliviar a quota da SP-API. Default = 2.0.",
    )
    parser.add_argument(
        "--root-filter",
        type=str,
        default=None,
        help="Opcional: filtra pelo(s) nome(s) da categoria raiz, separados por vírgula. "
             'Ex.: --root-filter "Pet Shop,Casa & Cozinha"',
    )
    parser.add_argument(
        "--start-from",
        type=int,
        default=0,
        help="Índice inicial na lista de tarefas (para continuar de onde parou em execuções anteriores).",
    )

    args = parser.parse_args()
    run_batch(
        max_items=args.max_items,
        max_tasks=args.max_tasks,
        sleep_seconds=args.sleep_seconds,
        root_filter=args.root_filter,
        start_from=args.start_from,
    )


if __name__ == "__main__":
    main()
