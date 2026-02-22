from __future__ import annotations

from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel


class MatchItem(BaseModel):
    created_at: datetime

    match_method: str
    match_score: float
    image_distance: Optional[int] = None

    asin: str
    amazon_title: Optional[str] = None
    amazon_brand: Optional[str] = None
    amazon_condition: Optional[str] = None
    amazon_price: Optional[float] = None
    amazon_currency: Optional[str] = None
    amazon_bsr: Optional[int] = None
    amazon_gtin: Optional[str] = None
    amazon_is_prime: Optional[int] = None  # tri-state (1/0/None)
    amazon_fulfillment: Optional[str] = None
    amazon_browse_node_name: Optional[str] = None
    amazon_image_url: Optional[str] = None
    amazon_url: Optional[str] = None

    item_id: str
    ebay_title: Optional[str] = None
    ebay_price: Optional[float] = None
    ebay_currency: Optional[str] = None
    ebay_condition: Optional[str] = None
    ebay_seller: Optional[str] = None
    ebay_url: Optional[str] = None

    # Arbitragem (Keepa-like): Amazon - eBay
    spread: Optional[float] = None
    spread_pct: Optional[float] = None


class MatchListResponse(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[MatchItem]