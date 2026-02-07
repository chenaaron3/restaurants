"""
Parser for generic HTML menus using Crawl4AI for markdown generation and LLM extraction.
"""
import re
from typing import List, Optional
from ...models import MenuItem, Menu
from .base import BaseParser
import instructor
from openai import OpenAI
from crawl4ai import AsyncWebCrawler
import os
from dotenv import load_dotenv

load_dotenv()


class HtmlLlmParser(BaseParser):
    """Parse HTML menus using Crawl4AI for markdown generation and LLM extraction"""
    
    def __init__(self):
        self.client = instructor.from_openai(OpenAI(api_key=os.getenv('OPENAI_API_KEY')))
    
    async def parse(self, url: str, html_content: Optional[str] = None) -> List[MenuItem]:
        """
        Parse menu from HTML using Crawl4AI for markdown generation and LLM extraction.
        
        Args:
            url: Restaurant website URL
            html_content: HTML content (required)
            
        Returns:
            List of MenuItem objects
        """
        if not html_content:
            return []
        
        try:
            # Use Crawl4AI to generate clean markdown from HTML
            menu_markdown = await self._generate_markdown_with_crawl4ai(url, html_content)
            
            if not menu_markdown:
                return []
            
            # Extract structured data using instructor
            menu_items = self._extract_with_llm(menu_markdown, url)
            
            return menu_items
            
        except Exception as e:
            print(f"Error in HTML+LLM parsing for {url}: {e}")
            return []
    
    def can_parse(self, url: str, html_content: Optional[str] = None) -> bool:
        """Check if HTML contains menu-like content"""
        if not html_content:
            return False
        
        # Check for menu indicators
        html_lower = html_content.lower()
        menu_keywords = ['menu', 'appetizer', 'entree', 'dessert', 'drink', 'price', '$']
        
        has_keywords = any(keyword in html_lower for keyword in menu_keywords)
        has_prices = bool(re.search(r'\$?\d+\.?\d{0,2}', html_content))
        
        return has_keywords and has_prices
    
    async def _generate_markdown_with_crawl4ai(self, url: str, html_content: str) -> Optional[str]:
        """
        Use Crawl4AI to generate clean markdown from HTML content.
        Crawl4AI handles HTML cleaning, content selection, and markdown generation.
        
        We use Crawl4AI with the original URL to leverage its full capabilities
        (dynamic content handling, better content selection, etc.).
        The pre-fetched HTML is available but Crawl4AI will fetch fresh content
        which may include dynamic elements that weren't in the initial fetch.
        """
        try:
            # Create a new crawler instance for this request
            # Using async with ensures proper cleanup
            async with AsyncWebCrawler() as crawler:
                # Use Crawl4AI with the original URL
                # This allows Crawl4AI to:
                # 1. Handle dynamic content (JavaScript-rendered menus)
                # 2. Apply intelligent content selection
                # 3. Generate optimized markdown for LLM consumption
                result = await crawler.arun(
                    url=url,
                    bypass_cache=True,  # Always fetch fresh content
                )
                
                if result and result.markdown:
                    return result.markdown
            
            return None
            
        except Exception as e:
            print(f"Error generating markdown with Crawl4AI: {e}")
            return None
    
    def _extract_with_llm(self, menu_markdown: str, url: str) -> List[MenuItem]:
        """
        Extract menu items from Crawl4AI-generated markdown using instructor.
        The markdown is already cleaned and optimized for LLM consumption.
        """
        try:
            prompt = f"""Extract all menu items from this menu content (generated from HTML using Crawl4AI).

Rules:
- Identify appetizers (starters, small plates), entrees (main dishes), and drinks (beverages)
- Extract item names, prices (as numbers, e.g., 12.99 not "$12.99"), descriptions
- Generate relevant tags from descriptions (e.g., "chicken curry with rice" → ["curry", "rice", "chicken"])
- Extract section names if available (e.g., "Appetizers", "Entrees", "Desserts")
- Return structured JSON matching the schema

Menu content (markdown):
{menu_markdown[:8000]}  # Limit to avoid token limits
"""
            
            menu = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                response_model=Menu,
                temperature=0
            )
            
            return menu.items
            
        except Exception as e:
            print(f"Error in LLM extraction: {e}")
            return []
    
