"""
Pydantic models for menu items and restaurant data.
"""
from pydantic import BaseModel, Field, validator
from typing import Literal, Optional, List
from datetime import datetime


class MenuItem(BaseModel):
    """
    Core dish model - represents a single menu item.
    
    Required fields ensure data completeness, optional fields provide
    additional context for better searchability and user experience.
    """
    # Required fields
    name: str = Field(..., description="Dish name (e.g., 'Caesar Salad')")
    price: float = Field(..., gt=0, description="Price in USD (e.g., 12.99)")
    type: Literal["appetizer", "entree", "drink"] = Field(
        ..., 
        description="Item category: appetizer (starters/small plates), entree (main dishes), drink (beverages)"
    )
    
    # Optional but highly recommended
    section: Optional[str] = Field(
        None,
        description="Menu section/category name (e.g., 'Appetizers', 'Entrees', 'Desserts', 'Cocktails')"
    )
    description: Optional[str] = Field(
        None,
        description="Item description (e.g., 'Fresh romaine lettuce with caesar dressing')"
    )
    tags: List[str] = Field(
        default_factory=list,
        description="Food tags for searchability (e.g., ['salads', 'vegetarian', 'gluten-free'])"
    )
    
    # Optional metadata
    dietary_info: Optional[List[str]] = Field(
        None,
        description="Dietary information (e.g., ['vegetarian', 'vegan', 'gluten-free', 'dairy-free'])"
    )
    
    @validator('price')
    def validate_price(cls, v):
        """Ensure price is positive and rounded to 2 decimals"""
        if v <= 0:
            raise ValueError('Price must be > 0')
        return round(float(v), 2)
    
    @validator('name')
    def normalize_name(cls, v):
        """Normalize whitespace in name"""
        return ' '.join(v.split())
    
    @validator('tags', 'dietary_info')
    def normalize_lists(cls, v):
        """Normalize list fields - remove empty strings, lowercase tags"""
        if v is None:
            return []
        return [item.strip().lower() for item in v if item.strip()]


class Menu(BaseModel):
    """Collection of menu items"""
    items: List[MenuItem] = Field(default_factory=list)


class RestaurantMenu(BaseModel):
    """Complete restaurant menu with metadata"""
    restaurant_id: str
    restaurant_name: str
    website_uri: Optional[str] = None
    platform_detected: Optional[str] = None
    extraction_date: datetime = Field(default_factory=datetime.utcnow)
    confidence_score: Optional[float] = Field(None, ge=0, le=1)
    menu: List[MenuItem] = Field(default_factory=list)
