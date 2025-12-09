"""
crawler_amazon.py

Pequeno "crawler" Amazon-first:
- Lê as categorias do search_tasks.yaml (via lib.tasks.load_categories_tree)
- Para cada (categoria, subcategoria) executa discover_amazon_products()
- Salva / atualiza os produtos na tabela amazon_products
- Respeita limites básicos de chamadas via parâmetros de max_items e pausas

Uso típico (a partir da raiz do projeto):

  python crawler_amazon.py --root "Pet Shop" --child "Cachorros" --max-items 200

Ou para rodar em todas as categorias do YAML (cuidado com a cota da SP-API):

  python crawler_amazon.py --max-items 200
"""

import argparse
import time
from typing import Optional, Dict, Any, List

import pandas as pd

from lib.config import make_engine
from lib.tasks import load_categories_tree
from integrations.amazon_matching import discover_amazon_products
from lib.db import upsert_amazon_products


def _iter_root_child_nodes(tree: List[Dict[str, Any]]):
    """
    Gera tuplas (root_name, child_name, node_dict) para cada combinação do YAML.
    Se uma categoria não tiver children, child_name vem como None.
    """
    for root in tree:
        root_name = root.get("name")
        children = root.get("children") or []
        if children:
            for ch in children:
                yield root_name, ch.get("name"), ch
        else:
            # sem subcategorias declaradas → usa o próprio root como node
            yield root_name, None, root


def _kw_for_node(node: Dict[str, Any]) -> str:
    """
    Recupera o amazon_kw da categoria/subcategoria;
    se estiver vazio, cai para o nome em PT mesmo.
    """
    return (node.get("amazon_kw") or node.get("name") or "").strip()


def run_crawl(
    root_filter: Optional[str],
    child_filter: Optional[str],
    max_items: int,
    sleep_between_calls: float,
    only_first_kw: bool = True,
):
    """
    Executa a mineração Amazon-first em lote, salvando direto no banco.

    root_filter  : se informado, processa apenas essa categoria raiz (PT).
    child_filter : se informado, processa apenas essa subcategoria (PT) dentro da raiz.
    max_items    : quantos ASINs distintos queremos *no máximo* por chamada de discover_amazon_products.
    sleep_between_calls : pausa entre chamadas pesadas (em segundos).
    only_first_kw: True = usa só o amazon_kw principal de cada node
                   (mais seguro para não estourar cota). Futuramente podemos
                   expandir para múltiplos sufixos.
    """
    print("Carregando árvore de categorias a partir do search_tasks.yaml ...")
    tree = load_categories_tree()
    engine = make_engine()

    # Sufixos extras (futuramente podemos ativar isso para ampliar cobertura)
    extra_tokens = [""] if only_first_kw else ["", "a", "e", "i", "o", "u"]

    total_combos = 0
    for _ in _iter_root_child_nodes(tree):
        total_combos += 1

    print(f"Total de combinações root/subcategoria encontradas no YAML: {total_combos}")
    print()

    for root_name, child_name, node in _iter_root_child_nodes(tree):
        # filtros opcionais (para testar uma categoria específica)
        if root_filter and root_name != root_filter:
            continue
        if child_filter and child_name != child_filter:
            continue

        print("=" * 80)
        if child_name:
            print(f"🔎 Categoria: {root_name}  |  Subcategoria: {child_name}")
        else:
            print(f"🔎 Categoria: {root_name} (sem subcategoria explícita)")

        base_kw = _kw_for_node(node)
        if not base_kw:
            print("  - Aviso: node sem amazon_kw e sem name; pulando...")
            continue

        print(f"  - amazon_kw base: {base_kw!r}")
        print(f"  - max_items por chamada: {max_items}")
        print()

        # a ideia: tentar algumas variações de keyword, se habilitado,
        # mas SEM ultrapassar muito a cota (por isso only_first_kw=True por padrão)
        for token in extra_tokens:
            if token:
                kw = f"{base_kw} {token}".strip()
            else:
                kw = base_kw

            print(f"    ▶ Rodando discover_amazon_products para kw={kw!r} ...")

            # Chamamos exatamente o mesmo fluxo da tela "Minerar",
            # mas sem browse_node_id (a API não aceita filtrar por isso)
            items, stats = discover_amazon_products(
                kw=kw,
                amazon_price_min=None,
                amazon_price_max=None,
                amazon_offer_type="any",
                min_monthly_sales_est=0,
                browse_node_id=None,   # não filtramos por classificationId
                max_items=max_items,
                progress_cb=None,      # aqui não precisamos de barra de progresso
            )

            kept = len(items)
            print(
                f"    → Resultado: kept={kept} | "
                f"catalog_seen={stats.get('catalog_seen')} | "
                f"with_price={stats.get('with_price')} | "
                f"errors_api={stats.get('errors_api')}"
            )

            last_err = stats.get("last_error")
            if last_err:
                print(f"      Último erro de API observado: {last_err}")

            if not items:
                print(
                    "    - Nenhum item mantido para essa keyword. "
                    "Indo para a próxima keyword/categoria."
                )
                # mesmo assim, pequena pausa para não spammar a API
                time.sleep(sleep_between_calls)
                continue

            # Monta DataFrame e adiciona metadados de origem (iguais aos da tela)
            df = pd.DataFrame(items)
            df["source_root_name"] = root_name
            df["source_child_name"] = child_name
            df["search_kw"] = kw

            try:
                inserted = upsert_amazon_products(engine, df)
                print(f"    ✅ upsert_amazon_products OK - linhas processadas: {inserted}")
            except Exception as e:
                print(
                    "    ⚠️ Falha ao salvar no banco "
                    "(mas a mineração em si funcionou): "
                    f"{e}"
                )

            # Pausa entre chamadas para respeitar a cota da SP-API
            if sleep_between_calls > 0:
                print(
                    f"    ⏸ Aguardando {sleep_between_calls} segundos "
                    "antes da próxima chamada..."
                )
                time.sleep(sleep_between_calls)

        print()  # linha em branco entre categorias


def main():
    parser = argparse.ArgumentParser(
        description="Crawler Amazon-first para preencher amazon_products."
    )
    parser.add_argument(
        "--root",
        dest="root",
        help="Nome da categoria raiz em PT (exato como no search_tasks.yaml), ex: 'Pet Shop'.",
    )
    parser.add_argument(
        "--child",
        dest="child",
        help="Nome da subcategoria em PT (exato como no search_tasks.yaml), ex: 'Cachorros'.",
    )
    parser.add_argument(
        "--max-items",
        dest="max_items",
        type=int,
        default=200,
        help="Máximo de ASINs distintos por chamada discover_amazon_products (default: 200).",
    )
    parser.add_argument(
        "--sleep",
        dest="sleep",
        type=float,
        default=2.0,
        help="Pausa (em segundos) entre chamadas de mineração (default: 2.0).",
    )
    parser.add_argument(
        "--all-variants",
        dest="all_variants",
        action="store_true",
        help=(
            "Usa várias variantes de keyword (base, base+' a', base+' e', ...). "
            "CUIDADO: aumenta o consumo de cota da SP-API."
        ),
    )

    args = parser.parse_args()

    run_crawl(
        root_filter=args.root,
        child_filter=args.child,
        max_items=args.max_items,
        sleep_between_calls=args.sleep,
        only_first_kw=not args.all_variants,
    )


if __name__ == "__main__":
    main()
