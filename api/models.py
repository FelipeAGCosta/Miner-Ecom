from __future__ import annotations

from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel


class MatchItem(BaseModel):
    created_at: datetime
    match_method: str
    match_score: float
    image_distance: Optional[int]

    asin: str
    amazon_title: Optional[str]
    amazon_brand: Optional[str]
    amazon_condition: Optional[str]
    amazon_price: Optional[float]
    amazon_currency: Optional[str]
    amazon_bsr: Optional[int]
    amazon_gtin: Optional[str]
    amazon_is_prime: Optional[int]          # tri-state (1/0/None)
    amazon_fulfillment: Optional[str]
    amazon_browse_node_name: Optional[str]
    amazon_image_url: Optional[str]
    amazon_url: str

    item_id: str
    ebay_title: Optional[str]
    ebay_price: Optional[float]
    ebay_currency: Optional[str]
    ebay_condition: Optional[str]
    ebay_seller: Optional[str]
    ebay_url: Optional[str]


class MatchListResponse(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[MatchItem]