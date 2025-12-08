#!/usr/bin/env python
# crawler_amazon_batch.py
#
# Varre várias categorias/subcategorias do search_tasks.yaml,
# chama discover_amazon_products para cada uma e salva / atualiza
# na tabela amazon_products.
#
# Uso típico:
#   python crawler_amazon_batch.py --root-filter "Pet Shop" --max-items 50 --max-tasks 2

import argparse
import sys
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from lib.config import make_engine
from lib.tasks import load_categories_tree
from integrations.amazon_matching import discover_amazon_products
from lib.db import upsert_amazon_products


# ----------------------------------------------------------------------
# Monta a lista de "tarefas" (root + child + kw) a partir do YAML
# ----------------------------------------------------------------------
def build_tasks(
    root_filter: Optional[str] = None,
    child_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Lê a árvore de categorias do search_tasks.yaml e retorna uma lista de tarefas:

      {
        "root_name": "Pet Shop",
        "child_name": "Cachorros",
        "kw": "dogs"  # amazon_kw da subcategoria (ou da raiz se não tiver filhos)
      }

    root_filter / child_filter são comparações EXATAS pelo campo "name" em PT.
    """
    tree = load_categories_tree()
    tasks: List[Dict[str, Any]] = []

    for root in tree:
        root_name = root.get("name")
        if not root_name:
            continue

        # aplica filtro de root, se informado
        if root_filter and root_name != root_filter:
            continue

        children = root.get("children") or []
        root_kw = (root.get("amazon_kw") or root_name).strip()

        if not children:
            # categoria sem filhos → vira uma tarefa sozinha
            if child_filter:
                # usuário pediu subcategoria específica, mas não existe aqui
                continue
            tasks.append(
                {
                    "root_name": root_name,
                    "child_name": None,
                    "kw": root_kw,
                }
            )
            continue

        # com filhos → cada child gera uma tarefa
        for ch in children:
            child_name = ch.get("name")
            if not child_name:
                continue

            if child_filter and child_name != child_filter:
                continue

            child_kw = (ch.get("amazon_kw") or child_name).strip()

            tasks.append(
                {
                    "root_name": root_name,
                    "child_name": child_name,
                    "kw": child_kw,
                }
            )

    # ordena só pra ficar previsível no log
    tasks.sort(key=lambda t: (t["root_name"] or "", t["child_name"] or ""))
    return tasks


# ----------------------------------------------------------------------
# Execução principal do batch
# ----------------------------------------------------------------------
def run_batch(
    root_filter: Optional[str],
    child_filter: Optional[str],
    max_items: int,
    max_tasks: int,
    sleep_seconds: float,
) -> None:
    engine = make_engine()

    tasks = build_tasks(root_filter=root_filter, child_filter=child_filter)
    if not tasks:
        print("Nenhuma tarefa encontrada para os filtros informados.")
        return

    if max_tasks <= 0 or max_tasks > len(tasks):
        max_tasks = len(tasks)

    print(
        f"\n📦 Iniciando batch Amazon-first "
        f"({max_tasks} tarefa(s), até {max_items} itens por tarefa)...\n"
    )

    for idx, task in enumerate(tasks[:max_tasks], start=1):
        root_name = task["root_name"]
        child_name = task["child_name"]
        kw_base = task["kw"]

        print(
            f"[{idx}/{max_tasks}] 🔎 Categoria: {root_name}"
            f" | Subcategoria: {child_name or '(nenhuma)'}"
        )
        print(f"   - amazon_kw base: '{kw_base}'")
        print(f"   - max_items para esta tarefa: {max_items}")

        # Chamada principal à Amazon (catalog + pricing)
        try:
            am_items, stats = discover_amazon_products(
                kw=kw_base,
                amazon_price_min=None,
                amazon_price_max=None,
                amazon_offer_type="any",
                min_monthly_sales_est=0,
                browse_node_id=None,  # se quiser, pode plugar category_id aqui depois
                max_items=max_items,
                progress_cb=None,  # batch de linha de comando, sem barra de progresso
            )
        except Exception as e:
            print(f"   ❌ Erro fatal em discover_amazon_products: {e}")
            break

        kept = stats.get("kept", len(am_items))
        catalog_seen = stats.get("catalog_seen", 0)
        with_price = stats.get("with_price", 0)
        errors_api = stats.get("errors_api", 0)
        last_error = stats.get("last_error") or ""

        print(
            f"   → Resultado: kept={kept} | catalog_seen={catalog_seen} "
            f"| with_price={with_price} | errors_api={errors_api}"
        )
        if last_error:
            print(f"     Último erro de API observado: {last_error}")

        # Se não veio nada, só pula pro próximo
        if not am_items:
            print("   (Nenhum item retornado pela Amazon para esta tarefa.)")
        else:
            df = pd.DataFrame(am_items)
            try:
                rows_ok = upsert_amazon_products(
                    engine,
                    df,
                    source_root_name=root_name,
                    source_child_name=child_name,
                    search_kw=kw_base,
                )
                print(f"   ✅ upsert_amazon_products OK - linhas processadas: {rows_ok}")
            except Exception as e:
                print(
                    "   ⚠️ Falha ao salvar no banco (mas a mineração em si funcionou): "
                    f"{e}"
                )

        # Se a Amazon deu QUOTAEXCEEDED, encerramos o batch de forma elegante
        if errors_api and "QUOTAEXCEEDED" in last_error.upper():
            print(
                "\n⚠️ A Amazon sinalizou QUOTAEXCEEDED (cota de API atingida). "
                "Encerrando este batch para não ficar batendo à toa.\n"
            )
            break

        # Pausa entre tarefas, se houver mais pela frente
        if idx < max_tasks and sleep_seconds > 0:
            print(
                f"   ⏸ Aguardando {sleep_seconds:.1f} segundos antes da próxima chamada...\n"
            )
            try:
                time.sleep(sleep_seconds)
            except KeyboardInterrupt:
                print("\nInterrompido manualmente pelo usuário.")
                break

    print("\n✅ Batch finalizado.\n")


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Batch de mineração Amazon-first (usa search_tasks.yaml)."
    )
    parser.add_argument(
        "--root-filter",
        type=str,
        default=None,
        help='Filtra pelo nome da categoria raiz (exato, em PT, ex.: "Pet Shop").',
    )
    parser.add_argument(
        "--child-filter",
        type=str,
        default=None,
        help='Filtra pelo nome da subcategoria (exato, em PT, ex.: "Cachorros").',
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=50,
        help="Número máximo de itens distintos (com preço) por tarefa (default: 50).",
    )
    parser.add_argument(
        "--max-tasks",
        type=int,
        default=10,
        help="Número máximo de tarefas a executar nesse batch (default: 10).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=2.0,
        help="Pausa em segundos entre uma tarefa e outra (default: 2.0).",
    )

    args = parser.parse_args(argv)

    run_batch(
        root_filter=args.root_filter,
        child_filter=args.child_filter,
        max_items=args.max_items,
        max_tasks=args.max_tasks,
        sleep_seconds=args.sleep_seconds,
    )


if __name__ == "__main__":
    main()
