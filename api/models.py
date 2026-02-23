from __future__ import annotations

from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel


class MatchItem(BaseModel):
    created_at: datetime

    # pode vir NULL do banco dependendo do caso (validated_method)
    match_method: Optional[str] = None
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
    amazon_is_prime: Optional[int] = None
    amazon_fulfillment: Optional[str] = None
    amazon_browse_node_name: Optional[str] = None
    amazon_image_url: Optional[str] = None
    amazon_url: Optional[str] = None

    amazon_category_root: Optional[str] = None
    amazon_category_child: Optional[str] = None

    item_id: str
    ebay_title: Optional[str] = None
    ebay_price: Optional[float] = None
    ebay_currency: Optional[str] = None
    ebay_condition: Optional[str] = None
    ebay_seller: Optional[str] = None
    ebay_url: Optional[str] = None
    ebay_available_qty: Optional[int] = None
    ebay_qty_flag: Optional[str] = None
    ebay_min_available_qty: Optional[int] = None
    ebay_availability_status: Optional[str] = None
    ebay_availability_threshold_type: Optional[str] = None
    ebay_availability_threshold: Optional[int] = None
    ebay_availability_updated_at: Optional[datetime] = None

    spread: Optional[float] = None
    spread_pct: Optional[float] = None

    class Config:
        extra = "ignore"  # evita 500 se SQL devolver campo a mais


class MatchListResponse(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[MatchItem]