# lib/tasks.py
"""
Leitura e utilitários para o arquivo de configuração `search_tasks.yaml`.

Responsabilidades:
- Carregar o YAML de tarefas/categorias a partir da raiz do projeto.
- Expor:
    - load_categories_tree() → lista crua (com children)
    - flatten_categories()   → DataFrame achatado (categoria/subcategoria)
    - load_tasks()           → DataFrame com bloco opcional `tasks:` do YAML

Observação:
- O próprio arquivo `search_tasks.yaml` NÃO é versionado (gitignored).
- Quem clona o projeto deve criar/ajustar seu próprio YAML na raiz.
"""

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml


def _load_yaml() -> Dict[str, Any]:
    """
    Lê o arquivo `search_tasks.yaml` na raiz do projeto.

    Retorno:
        - dict com o conteúdo do YAML, ou {} se o arquivo não existir
          ou estiver vazio.
    """
    cfg_path = Path(__file__).resolve().parents[1] / "search_tasks.yaml"
    if not cfg_path.exists():
        return {}

    with cfg_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    # Garante sempre um dict como raiz (caso o YAML seja uma lista solta, por exemplo)
    if isinstance(data, dict):
        return data
    return {}


def load_categories_tree() -> List[Dict[str, Any]]:
    """
    Retorna a lista crua de categorias (com children) a partir do YAML.

    Espera-se que o YAML tenha uma chave de topo:

        categories:
          - name: "Pet Shop"
            amazon_kw: "pet supplies"
            category_id: 123
            children:
              - name: "Cachorros"
                amazon_kw: "dog supplies"
                category_id: 456
              ...

    Se `categories` não existir, retorna lista vazia.
    """
    data = _load_yaml()
    categories = data.get("categories", []) or []
    return categories if isinstance(categories, list) else []


def flatten_categories(tree: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Achata a árvore de categorias em linhas:

        name, amazon_kw, category_id (opcional), parent

    Onde:
      - `name`        → nome da categoria ou subcategoria
      - `amazon_kw`   → keyword principal para Amazon
      - `category_id` → opcional (pode vir do YAML, mantido como Int64)
      - `parent`      → nome da categoria-mãe (ou None para roots)

    O objetivo é facilitar a exibição e seleção na UI.
    """
    rows: List[Dict[str, Any]] = []

    for node in tree:
        # Categoria raiz
        rows.append(
            {
                "name": node.get("name"),
                "amazon_kw": node.get("amazon_kw"),
                "category_id": node.get("category_id"),
                "parent": None,
            }
        )

        # Filhos / subcategorias
        for ch in (node.get("children") or []) or []:
            rows.append(
                {
                    "name": ch.get("name"),
                    "amazon_kw": ch.get("amazon_kw"),
                    "category_id": ch.get("category_id"),
                    "parent": node.get("name"),
                }
            )

    df = pd.DataFrame(rows)

    # category_id como Int64 (permite NA) se a coluna existir
    if not df.empty and "category_id" in df.columns:
        df["category_id"] = (
            pd.to_numeric(df["category_id"], errors="coerce")
            .astype("Int64")
        )

    return df


def load_tasks() -> pd.DataFrame:
    """
    Carrega o bloco opcional `tasks:` do YAML como DataFrame.

    Exemplo de estrutura suportada:

        tasks:
          - name: "Debug GTINs Pet"
            amazon_kw: "dog bone"
            category_id: 123
          - name: "Test Eletrônicos"
            amazon_kw: "wireless headphones"

    Se `tasks` não existir ou estiver vazio, retorna DataFrame vazio.
    """
    data = _load_yaml()
    tasks = data.get("tasks", []) or []
    if not isinstance(tasks, list):
        return pd.DataFrame()
    return pd.DataFrame(tasks)
