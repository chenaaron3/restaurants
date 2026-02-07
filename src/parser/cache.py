"""
Caching logic for menu data.
"""
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
from ..models import RestaurantMenu


class MenuCache:
    """Handle caching of menu data"""
    
    def __init__(self, cache_dir: str = "cache/menus"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_ttl_days = 30
    
    def get_cache_path(self, restaurant_id: str) -> Path:
        """Get cache file path for restaurant"""
        return self.cache_dir / f"{restaurant_id}.json"
    
    def load(self, restaurant_id: str) -> Optional[RestaurantMenu]:
        """Load menu from cache if fresh"""
        cache_path = self.get_cache_path(restaurant_id)
        
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)
            
            # Check if cache is fresh
            extraction_date = datetime.fromisoformat(data.get('extraction_date', ''))
            if datetime.utcnow() - extraction_date > timedelta(days=self.cache_ttl_days):
                return None
            
            return RestaurantMenu(**data)
            
        except Exception as e:
            print(f"Error loading cache for {restaurant_id}: {e}")
            return None
    
    def save(self, menu: RestaurantMenu):
        """Save menu to cache"""
        cache_path = self.get_cache_path(menu.restaurant_id)
        
        try:
            with open(cache_path, 'w') as f:
                json.dump(menu.dict(), f, indent=2, default=str)
        except Exception as e:
            print(f"Error saving cache for {menu.restaurant_id}: {e}")
    
    def is_cached(self, restaurant_id: str) -> bool:
        """Check if restaurant menu is cached and fresh"""
        cache_path = self.get_cache_path(restaurant_id)
        
        if not cache_path.exists():
            return False
        
        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)
            
            extraction_date = datetime.fromisoformat(data.get('extraction_date', ''))
            return datetime.utcnow() - extraction_date <= timedelta(days=self.cache_ttl_days)
            
        except Exception:
            return False
