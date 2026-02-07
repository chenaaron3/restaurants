"""
Main script for parsing restaurant menus.
"""
import asyncio
import json
import os
from pathlib import Path
from typing import List, Optional
from dotenv import load_dotenv
from tqdm.asyncio import tqdm

from ..models import RestaurantMenu, MenuItem
from ..parser.parser_factory import ParserFactory
from ..parser.menu_discovery import MenuDiscovery
from ..parser.cache import MenuCache

load_dotenv()


class MenuParser:
    """Main orchestrator for menu parsing"""
    
    def __init__(self, input_file: str, output_file: str = "output/restaurant_menus.json"):
        self.input_file = Path(input_file)
        self.output_file = Path(output_file)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.parser_factory = ParserFactory()
        self.menu_discovery = MenuDiscovery()
        self.cache = MenuCache()
    
    async def parse_restaurant(self, restaurant: dict) -> Optional[RestaurantMenu]:
        """Parse menu for a single restaurant"""
        restaurant_id = restaurant.get('place_id') or restaurant.get('id')
        restaurant_name = restaurant.get('displayName', {}).get('text') or restaurant.get('name', 'Unknown')
        website_uri = restaurant.get('websiteUri')
        
        if not website_uri:
            print(f"No website URI for {restaurant_name}")
            return None
        
        # Check cache
        cached_menu = self.cache.load(restaurant_id)
        if cached_menu:
            return cached_menu
        
        try:
            # Discover menu URL and fetch content
            # This handles navigation to menu pages, clicking buttons, etc.
            menu_url, html_content = await self.menu_discovery.get_menu_content(website_uri)
            
            if not menu_url:
                print(f"Could not find menu URL for {restaurant_name}")
                return None
            
            # Route to appropriate parser based on URL type
            if menu_url.lower().endswith('.pdf'):
                # PDF parsing
                menu_items = await self.parser_factory.pdf_parser.parse(menu_url)
            elif self._is_image_url(menu_url):
                # Image parsing (menu embedded in image)
                menu_items = await self.parser_factory.image_parser.parse(menu_url)
            elif html_content:
                # Parse HTML menu with extruct first, fallback to html_llm
                menu_items = await self._parse_with_fallback(menu_url, html_content)
            else:
                print(f"Could not fetch menu content for {restaurant_name}")
                return None
            
            # All parsers return List[MenuItem] per contract
            # Filter out None values and ensure all items are MenuItem instances
            valid_items = [item for item in menu_items if isinstance(item, MenuItem)]
            
            # Create restaurant menu
            restaurant_menu = RestaurantMenu(
                restaurant_id=restaurant_id,
                restaurant_name=restaurant_name,
                website_uri=menu_url,  # Use discovered menu URL
                platform_detected=None,  # No longer tracking platform separately
                menu=valid_items,
                confidence_score=self._calculate_confidence(valid_items)
            )
            
            # Save to cache
            self.cache.save(restaurant_menu)
            
            return restaurant_menu
            
        except Exception as e:
            print(f"Error parsing {restaurant_name}: {e}")
            return None
    
    def _is_image_url(self, url: str) -> bool:
        """Check if URL is an image"""
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in image_extensions)
    
    async def _parse_with_fallback(self, url: str, html_content: str) -> List[MenuItem]:
        """Parse menu with extruct first, fallback to html_llm"""
        # Try extruct parser first
        extruct_parser = self.parser_factory.extruct_parser
        if extruct_parser.can_parse(url, html_content):
            items = await extruct_parser.parse(url, html_content)
            if items:
                return items
        
        # Fallback to HTML+LLM parser
        html_llm_parser = self.parser_factory.html_llm_parser
        if html_llm_parser.can_parse(url, html_content):
            return await html_llm_parser.parse(url, html_content)
        
        return []
    
    def _calculate_confidence(self, items: List[MenuItem]) -> float:
        """Calculate confidence score based on data completeness"""
        if not items:
            return 0.0
        
        total_score = 0.0
        for item in items:
            score = 0.5  # Base score for having item
            
            if item.description:
                score += 0.2
            if item.section:
                score += 0.15
            if item.tags:
                score += 0.15
            
            total_score += min(score, 1.0)
        
        return total_score / len(items) if items else 0.0
    
    async def parse_all(self, max_concurrent: int = 10):
        """Parse menus for all restaurants"""
        # Load restaurants
        with open(self.input_file, 'r') as f:
            restaurants = json.load(f)
        
        if not isinstance(restaurants, list):
            # Assume it's a dict with restaurants
            restaurants = restaurants.get('restaurants', [])
        
        # Process restaurants
        results = []
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_restaurant(restaurant):
            async with semaphore:
                return await self.parse_restaurant(restaurant)
        
        tasks = [process_restaurant(r) for r in restaurants]
        
        for coro in tqdm.as_completed(tasks, desc="Parsing menus"):
            result = await coro
            if result:
                results.append(result)
        
        # Save results
        output_data = [menu.dict() for menu in results]
        with open(self.output_file, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        
        print(f"\nParsed {len(results)} menus. Saved to {self.output_file}")


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Parse restaurant menus')
    parser.add_argument(
        '--input',
        default='manhattan_restaurants.json',
        help='Input JSON file with restaurants'
    )
    parser.add_argument(
        '--output',
        default='output/restaurant_menus.json',
        help='Output JSON file for menus'
    )
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=10,
        help='Maximum concurrent requests'
    )
    
    args = parser.parse_args()
    
    parser_instance = MenuParser(args.input, args.output)
    await parser_instance.parse_all(max_concurrent=args.max_concurrent)


if __name__ == '__main__':
    asyncio.run(main())
