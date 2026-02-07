"""
Parser for extracting structured data (JSON-LD, microdata, RDFa) from HTML using extruct.
Uses LLM to fill in type and tags for ALL items extracted by extruct.
Extruct data (name, price, description, section, dietary_info) takes precedence and is trusted.

Uses ONLY standardized schema.org properties:
- MenuItem: name, description, offers (with price/priceCurrency), suitableForDiet
- Menu: hasMenuSection
- MenuSection: name, hasMenuItem, hasMenuSection (for nested sections)
- Offer: price, priceCurrency

Reference: https://schema.org/MenuItem
"""
import extruct
import json
from typing import List, Optional, Dict, Any
from ...models import MenuItem, Menu
from .base import BaseParser
import instructor
from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()

# Standardized schema.org property names
SCHEMA_MENU_ITEM = 'MenuItem'
SCHEMA_MENU = 'Menu'
SCHEMA_MENU_SECTION = 'MenuSection'
SCHEMA_PRODUCT = 'Product'  # Sometimes used instead of MenuItem
SCHEMA_FOOD_ESTABLISHMENT = 'FoodEstablishment'

# Standardized property names
PROP_NAME = 'name'  # From Thing
PROP_DESCRIPTION = 'description'  # From Thing
PROP_OFFERS = 'offers'  # From MenuItem
PROP_HAS_MENU = 'hasMenu'  # From Restaurant/FoodEstablishment
PROP_HAS_MENU_SECTION = 'hasMenuSection'  # From Menu/MenuSection
PROP_HAS_MENU_ITEM = 'hasMenuItem'  # From Menu/MenuSection
PROP_SUITABLE_FOR_DIET = 'suitableForDiet'  # From MenuItem

# Offer properties (nested in offers)
PROP_PRICE = 'price'  # From Offer
PROP_PRICE_CURRENCY = 'priceCurrency'  # From Offer

# JSON-LD specific
JSONLD_TYPE = '@type'


class ExtructParser(BaseParser):
    """Extract menu items from structured data in HTML, using LLM to fill in type and tags for all items"""
    
    def __init__(self):
        self.supported_formats = ['json-ld', 'microdata', 'rdfa']
        self.client = instructor.from_openai(OpenAI(api_key=os.getenv('OPENAI_API_KEY'))) if os.getenv('OPENAI_API_KEY') else None
    
    async def parse(self, url: str, html_content: Optional[str] = None) -> List[MenuItem]:
        """
        Extract menu items from structured data.
        
        Args:
            url: Restaurant website URL
            html_content: HTML content (required)
            
        Returns:
            List of MenuItem objects
        """
        if not html_content:
            return []
        
        try:
            # Extract structured data
            data = extruct.extract(html_content, uniform=True)
            
            menu_items = []
            semi_structured_data = []
            
            # Process JSON-LD data
            if 'json-ld' in data:
                items, semi = self._extract_from_jsonld(data['json-ld'])
                menu_items.extend(items)
                semi_structured_data.extend(semi)
            
            # Process microdata
            if 'microdata' in data:
                items, semi = self._extract_from_microdata(data['microdata'])
                menu_items.extend(items)
                semi_structured_data.extend(semi)
            
            # Process RDFa
            if 'rdfa' in data:
                items, semi = self._extract_from_rdfa(data['rdfa'])
                menu_items.extend(items)
                semi_structured_data.extend(semi)
            
            # Use LLM to refine ALL items (complete and semi-structured) for type and tags
            # Extruct data (name, price, description) takes precedence, LLM fills type/tags
            if menu_items and self.client:
                refined_items = self._refine_items_with_llm(menu_items, semi_structured_data)
                return refined_items
            
            # No items found or LLM client unavailable
            return []
            
        except Exception as e:
            print(f"Error in extruct parsing for {url}: {e}")
            return []
    
    def can_parse(self, url: str, html_content: Optional[str] = None) -> bool:
        """Check if structured data exists in HTML"""
        if not html_content:
            return False
        
        try:
            data = extruct.extract(html_content, uniform=True)
            # Check if any structured data exists
            return bool(data.get('json-ld') or data.get('microdata') or data.get('rdfa'))
        except Exception:
            return False
    
    def _extract_from_jsonld(self, jsonld_data: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Extract menu items from JSON-LD data, return (parsed_items, semi_structured_data)"""
        items = []
        semi_structured = []
        
        for item in jsonld_data:
            item_type = item.get(JSONLD_TYPE)
            
            # Check for MenuItem directly
            if item_type == SCHEMA_MENU_ITEM:
                menu_item = self._parse_menu_item(item)
                if menu_item:
                    items.append(menu_item)
                elif self._is_semi_structured(item):
                    # Preserve raw data for LLM processing
                    item['raw_data'] = item.copy()
                    semi_structured.append(item)
            
            # Check for nested menu structures (Restaurant -> Menu -> MenuSection -> MenuItem)
            if PROP_HAS_MENU in item:
                menu = item[PROP_HAS_MENU]
                if isinstance(menu, dict):
                    items_from_menu, semi_from_menu = self._extract_from_menu(menu)
                    items.extend(items_from_menu)
                    semi_structured.extend(semi_from_menu)
        
        return items, semi_structured
    
    def _extract_from_menu(self, menu: Dict[str, Any]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Extract items from Menu object (handles MenuSection nesting)"""
        items = []
        semi_structured = []
        
        # Get menu sections
        sections = menu.get(PROP_HAS_MENU_SECTION, [])
        if not isinstance(sections, list):
            sections = [sections] if sections else []
        
        for section in sections:
            if not isinstance(section, dict):
                continue
            
            section_name = section.get(PROP_NAME)  # Standardized: MenuSection.name
            
            # Get menu items from this section
            menu_items = section.get(PROP_HAS_MENU_ITEM, [])
            if not isinstance(menu_items, list):
                menu_items = [menu_items] if menu_items else []
            
            for menu_item_data in menu_items:
                parsed_item = self._parse_menu_item(menu_item_data, section_name=section_name)
                if parsed_item:
                    items.append(parsed_item)
                elif self._is_semi_structured(menu_item_data):
                    # Preserve section name and raw data for LLM processing
                    menu_item_data['_section_name'] = section_name
                    menu_item_data['raw_data'] = menu_item_data.copy()
                    semi_structured.append(menu_item_data)
            
            # Handle nested MenuSections (e.g., Dinner -> Starters -> MenuItem)
            nested_sections = section.get(PROP_HAS_MENU_SECTION, [])
            if nested_sections:
                if not isinstance(nested_sections, list):
                    nested_sections = [nested_sections]
                for nested_section in nested_sections:
                    nested_items, nested_semi = self._extract_from_menu_section(nested_section, parent_section=section_name)
                    items.extend(nested_items)
                    semi_structured.extend(nested_semi)
        
        return items, semi_structured
    
    def _extract_from_menu_section(self, section: Dict[str, Any], parent_section: Optional[str] = None) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Extract items from a MenuSection"""
        items = []
        semi_structured = []
        
        section_name = section.get(PROP_NAME) or parent_section
        
        menu_items = section.get(PROP_HAS_MENU_ITEM, [])
        if not isinstance(menu_items, list):
            menu_items = [menu_items] if menu_items else []
        
        for menu_item_data in menu_items:
            parsed_item = self._parse_menu_item(menu_item_data, section_name=section_name)
            if parsed_item:
                items.append(parsed_item)
            elif self._is_semi_structured(menu_item_data):
                menu_item_data['_section_name'] = section_name
                menu_item_data['raw_data'] = menu_item_data.copy()
                semi_structured.append(menu_item_data)
        
        return items, semi_structured
    
    def _extract_from_microdata(self, microdata: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Extract menu items from microdata, return (parsed_items, semi_structured_data)"""
        items = []
        semi_structured = []
        
        for item in microdata:
            item_type = item.get('type')  # microdata uses 'type' not '@type'
            
            if item_type == SCHEMA_MENU_ITEM or item_type == SCHEMA_PRODUCT:
                menu_item = self._parse_menu_item(item)
                if menu_item:
                    items.append(menu_item)
                elif self._is_semi_structured(item):
                    # Preserve raw data for LLM processing
                    item['raw_data'] = item.copy()
                    semi_structured.append(item)
            
            # Check for menu structures
            if PROP_HAS_MENU in item:
                menu = item[PROP_HAS_MENU]
                if isinstance(menu, dict):
                    menu_items, menu_semi = self._extract_from_menu(menu)
                    items.extend(menu_items)
                    semi_structured.extend(menu_semi)
        
        return items, semi_structured
    
    def _extract_from_rdfa(self, rdfa_data: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Extract menu items from RDFa data, return (parsed_items, semi_structured_data)"""
        items = []
        semi_structured = []
        
        for item in rdfa_data:
            # RDFa uses full URIs: http://schema.org/MenuItem
            item_type = str(item.get('type', ''))
            if 'schema.org/MenuItem' in item_type:
                menu_item = self._parse_menu_item(item)
                if menu_item:
                    items.append(menu_item)
                elif self._is_semi_structured(item):
                    # Preserve raw data for LLM processing
                    item['raw_data'] = item.copy()
                    semi_structured.append(item)
        
        return items, semi_structured
    
    def _parse_menu_item(self, data: Dict[str, Any], section_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Parse a single menu item from structured data using ONLY standardized schema.org properties.
        
        Returns raw extruct data (dict), not MenuItem. LLM will fill in type and tags.
        Extruct data (name, price, description) takes precedence.
        
        Standardized properties used:
        - name (from Thing)
        - description (from Thing)
        - offers.price (from Offer, nested in MenuItem.offers)
        - offers.priceCurrency (from Offer, nested in MenuItem.offers)
        - suitableForDiet (from MenuItem)
        """
        try:
            # Extract name - standardized: Thing.name
            name = data.get(PROP_NAME, '').strip()
            if not name:
                return None
            
            # Extract price - standardized: MenuItem.offers.price
            price = self._extract_price(data)
            if price is None or price <= 0:
                return None
            
            # Extract description - standardized: Thing.description
            description = data.get(PROP_DESCRIPTION)
            if description:
                description = description.strip() if isinstance(description, str) else None
            
            # Extract section name (from parent MenuSection.name, passed as parameter)
            section = section_name or data.get('_section_name')
            
            # Extract dietary info - standardized: MenuItem.suitableForDiet
            dietary_info = self._extract_dietary_info(data)
            
            # Return raw extruct data for LLM processing
            # LLM will fill in type and tags, extruct data (name, price, description) takes precedence
            return {
                'name': name,
                'price': price,
                'description': description,
                'section': section,
                'dietary_info': dietary_info,
                'raw_data': data,  # Keep original extruct data for LLM context
            }
            
        except Exception as e:
            print(f"Error parsing menu item: {e}")
            return None
    
    def _is_semi_structured(self, data: Dict[str, Any]) -> bool:
        """
        Check if data is semi-structured (has some standardized fields but incomplete).
        Uses ONLY standardized schema.org properties.
        """
        # Has name - standardized: Thing.name
        has_name = bool(data.get(PROP_NAME))
        
        # Missing critical fields
        missing_price = not self._extract_price(data)
        
        # Consider semi-structured if has name but missing price
        return has_name and missing_price
    
    def _refine_items_with_llm(self, parsed_items: List[Dict[str, Any]], semi_structured_data: List[Dict[str, Any]]) -> List[MenuItem]:
        """
        Use LLM to refine ALL extruct items - fill in type and tags.
        Extruct data (name, price, description) takes precedence and is trusted.
        LLM only fills in: type, tags
        
        Flow:
        1. extruct extracts structured data (name, price, description, section, dietary_info)
        2. Pass raw extruct data to LLM
        3. LLM fills in type and tags based on extruct data
        4. Merge: extruct values for name/price/description, LLM values for type/tags
        """
        if not self.client:
            return []
        
        try:
            # Combine all items (complete and semi-structured)
            all_items = parsed_items + semi_structured_data
            
            # Filter out None values
            all_items = [item for item in all_items if item is not None]
            
            if not all_items:
                return []
            
            # Prepare data for LLM - preserve extruct values
            items_for_llm = []
            for item in all_items:
                if isinstance(item, dict):
                    raw_data = item.get('raw_data', item)
                    
                    # Extract reliable extruct data
                    # For semi-structured items, try to extract price from raw_data
                    price = item.get('price')
                    if price is None and raw_data:
                        price = self._extract_price(raw_data)
                    
                    llm_item = {
                        'name': item.get('name') or raw_data.get(PROP_NAME, '') if isinstance(raw_data, dict) else '',
                        'price': price,
                        'description': item.get('description') or (raw_data.get(PROP_DESCRIPTION) if isinstance(raw_data, dict) else None),
                        'section': item.get('section') or item.get('_section_name'),
                        'dietary_info': item.get('dietary_info'),
                        # Include raw extruct data for context
                        'raw_extruct_data': raw_data,
                    }
                    # Only include items with name and price (required fields)
                    if llm_item.get('name') and llm_item.get('price'):
                        items_for_llm.append(llm_item)
            
            if not items_for_llm:
                return []
            
            # Convert to JSON for LLM
            data_json = json.dumps(items_for_llm, indent=2, default=str)
            
            prompt = f"""You are processing menu items extracted from schema.org structured markup (JSON-LD, microdata, RDFa).

The extruct library has already extracted reliable data from the structured markup. Your job is to fill in ONLY the missing fields: type and tags.

CRITICAL RULES:
1. **TRUST extruct data** - Use name, price, description, section, dietary_info exactly as provided
2. **DO NOT change** name, price, description, section, dietary_info values
3. **ONLY infer** the following fields:
   - type: Must be one of ["appetizer", "entree", "drink"] - infer from section name, item name, or description
   - tags: Generate relevant food tags (e.g., ["salads", "rice", "noodle", "curry", "soup", "vegetarian", "spicy", "seafood"]) based on description and name

Standardized schema.org properties already extracted (USE THESE VALUES AS-IS):
- name (Thing.name) - DO NOT CHANGE
- description (Thing.description) - DO NOT CHANGE  
- price (from MenuItem.offers.price) - DO NOT CHANGE
- section (from MenuSection.name) - DO NOT CHANGE
- dietary_info (from MenuItem.suitableForDiet) - DO NOT CHANGE

Extracted menu items from extruct:
{data_json[:8000]}  # Limit to avoid token limits
"""
            
            menu = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                response_model=Menu,
                temperature=0
            )
            
            # Merge LLM output with extruct data (extruct takes precedence for name/price/description)
            final_items = []
            for i, llm_item in enumerate(menu.items):
                if i >= len(items_for_llm):
                    continue
                
                extruct_item = items_for_llm[i]
                
                # Extruct data takes precedence (more reliable from structured markup)
                # LLM only provides type and tags
                final_item = MenuItem(
                    name=extruct_item.get('name') or llm_item.name,  # Extruct first
                    price=extruct_item.get('price') or llm_item.price,  # Extruct first
                    type=llm_item.type,  # From LLM (not in schema.org)
                    section=extruct_item.get('section') or llm_item.section,  # Extruct first
                    description=extruct_item.get('description') or llm_item.description,  # Extruct first
                    tags=llm_item.tags,  # From LLM (not in schema.org)
                    dietary_info=extruct_item.get('dietary_info') or llm_item.dietary_info,  # Extruct first
                )
                final_items.append(final_item)
            
            return final_items
            
        except Exception as e:
            print(f"Error refining with LLM: {e}")
            return []
    
    def _extract_price(self, data: Dict[str, Any]) -> Optional[float]:
        """
        Extract price from structured data using ONLY standardized schema.org properties.
        
        Standardized path: MenuItem.offers.price (where offers is an Offer object)
        Reference: https://schema.org/Offer
        """
        # Standardized: MenuItem.offers (Offer object with price and priceCurrency)
        offers = data.get(PROP_OFFERS)
        if offers:
            if isinstance(offers, dict):
                # Standardized: Offer.price
                price_str = offers.get(PROP_PRICE)
                if price_str:
                    return self._parse_price(price_str)
            elif isinstance(offers, list) and offers:
                # Handle list of offers (take first)
                first_offer = offers[0]
                if isinstance(first_offer, dict):
                    price_str = first_offer.get(PROP_PRICE)
                    if price_str:
                        return self._parse_price(price_str)
        
        return None
    
    def _extract_dietary_info(self, data: Dict[str, Any]) -> Optional[List[str]]:
        """
        Extract dietary information using standardized schema.org property.
        
        Standardized: MenuItem.suitableForDiet (RestrictedDiet)
        Reference: https://schema.org/suitableForDiet
        """
        diet = data.get(PROP_SUITABLE_FOR_DIET)
        if not diet:
            return None
        
        # Can be a string (URL or text) or list
        if isinstance(diet, str):
            # Extract diet name from URL (e.g., "https://schema.org/GlutenFreeDiet" -> "gluten-free")
            if 'schema.org/' in diet:
                diet_name = diet.split('/')[-1].replace('Diet', '').lower()
                return [diet_name]
            return [diet.lower()]
        elif isinstance(diet, list):
            return [str(d).lower() for d in diet]
        
        return None
    
    def _parse_price(self, price_str: str) -> Optional[float]:
        """Parse price string to float"""
        import re
        # Remove currency symbols and extract number
        price_match = re.search(r'[\d.]+', str(price_str).replace(',', ''))
        if price_match:
            try:
                return float(price_match.group())
            except ValueError:
                pass
        return None
    
