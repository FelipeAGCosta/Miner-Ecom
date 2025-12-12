# lib/bsr_heuristics.py
from typing import Optional

def estimate_monthly_sales(bsr: Optional[int], category: str | None) -> Optional[int]:
    """
    Versão pública/simplificada da heurística.
    Só para demo / repositório público.
    """
    if bsr is None:
        return None

    if bsr <= 1_000:
        return 500
    elif bsr <= 5_000:
        return 300
    elif bsr <= 20_000:
        return 100
    elif bsr <= 100_000:
        return 30
    else:
        return 5
