from pathlib import Path
import pandas as pd
import yaml

def _load_yaml() -> dict:
    cfg_path = Path(__file__).resolve().parents[1] / "search_tasks.yaml"
    if not cfg_path.exists():
        return {}
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def load_categories_tree() -> list[dict]:
    """Retorna a lista crua (com children) do YAML."""
    data = _load_yaml()
    return data.get("categories", []) or []

def flatten_categories(tree: list[dict]) -> pd.DataFrame:
    """
    Achata a árvore em linhas: name, amazon_kw, category_id (opcional), parent_name.
    Agora category_id pode não existir; mantemos como string/Int64 se vier.
    """
    rows = []
    for node in tree:
        rows.append(
            {
                "name": node.get("name"),
                "amazon_kw": node.get("amazon_kw"),
                "category_id": node.get("category_id"),
                "parent": None,
            }
        )
        for ch in node.get("children", []) or []:
            rows.append(
                {
                    "name": ch.get("name"),
                    "amazon_kw": ch.get("amazon_kw"),
                    "category_id": ch.get("category_id"),
                    "parent": node.get("name"),
                }
            )
    df = pd.DataFrame(rows)
    if not df.empty and "category_id" in df.columns:
        df["category_id"] = pd.to_numeric(df["category_id"], errors="coerce").astype("Int64")
    return df

def load_tasks() -> pd.DataFrame:
    data = _load_yaml()
    return pd.DataFrame(data.get("tasks", []) or [])
