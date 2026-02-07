"""
Menu discovery and navigation - finds menu content on restaurant websites.
Handles simple links, button clicking, and image-based menus.
"""
from typing import Optional, List, Tuple
import httpx
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
from crawl4ai import AsyncWebCrawler


class MenuDiscovery:
    """Discovers and navigates to menu content on restaurant websites"""
    
    def __init__(self):
        pass
    
    async def get_menu_content(self, entrypoint_url: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Get menu URL and HTML content, handling links, button clicks, and images.
        
        Args:
            entrypoint_url: Restaurant website entrypoint
            
        Returns:
            Tuple of (menu_url, html_content)
            For PDFs: (pdf_url, None) - PDF parser will download directly
            For HTML: (html_url, html_content)
            For images: (image_url, None) - Will be handled by vision API
        """
        try:
            # First, try simple HTTP approach (fastest)
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(entrypoint_url)
                response.raise_for_status()
                html_content = response.text
                
                if not html_content:
                    return None, None
                
                # Check if current page already has menu content
                if self._has_menu_content(html_content):
                    # Check if menu is in an image
                    image_url = self._find_menu_image(html_content, entrypoint_url)
                    if image_url:
                        return image_url, None
                    return entrypoint_url, html_content
                
                # Extract and prioritize menu links
                menu_urls = self._extract_menu_links(html_content, entrypoint_url)
                
                # Try each menu URL found (prioritized by link text)
                for menu_url in menu_urls:
                    try:
                        # Handle PDFs
                        if menu_url.lower().endswith('.pdf'):
                            return menu_url, None
                        
                        # Fetch and check HTML content
                        menu_response = await client.get(menu_url, timeout=30.0)
                        menu_response.raise_for_status()
                        menu_html = menu_response.text
                        
                        if menu_html and self._has_menu_content(menu_html):
                            # Check if menu is in an image on this page
                            image_url = self._find_menu_image(menu_html, menu_url)
                            if image_url:
                                return image_url, None
                            return menu_url, menu_html
                    except:
                        continue
                
                # If simple HTTP didn't work, try browser automation for button clicking
                return await self._try_browser_automation(entrypoint_url)
                
        except Exception as e:
            print(f"Error discovering menu content for {entrypoint_url}: {e}")
            return None, None
    
    async def _try_browser_automation(self, entrypoint_url: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Use browser automation to handle JavaScript-rendered menus and button clicks.
        """
        try:
            async with AsyncWebCrawler() as crawler:
                result = await crawler.arun(url=entrypoint_url, bypass_cache=True)
                
                if not result or not result.html:
                    return None, None
                
                # Check if we now have menu content after JavaScript execution
                if self._has_menu_content(result.html):
                    # Check for menu images
                    image_url = self._find_menu_image(result.html, entrypoint_url)
                    if image_url:
                        return image_url, None
                    return entrypoint_url, result.html
                
                # Try to find and click menu buttons
                # Look for buttons/links with menu-related text
                soup = BeautifulSoup(result.html, 'html.parser')
                menu_buttons = soup.find_all(['button', 'a', 'div'], 
                                             string=re.compile(r'menu|order', re.IGNORECASE))
                
                # If we found menu buttons but no content, the page might need interaction
                # For now, return the page content - Crawl4AI should have executed JS
                # In the future, we could add explicit button clicking here
                return entrypoint_url, result.html
                
        except Exception as e:
            print(f"Error in browser automation for {entrypoint_url}: {e}")
            return None, None
    
    def _extract_menu_links(self, html_content: str, base_url: str) -> List[str]:
        """
        Extract menu-related links from HTML, prioritized by link text.
        Priority: "menu" > "order" > PDF links (all PDFs included)
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        priority_links = []  # Links with "menu" or "order" text
        pdf_links = []       # All PDF links (may be menus)
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').strip()
            if not href or href.startswith('#'):
                continue
                
            link_text = link.get_text(strip=True).lower()
            full_url = urljoin(base_url, href)
            
            # Priority 1: Links with "menu" or "order" text
            if 'menu' in link_text or 'order' in link_text:
                if full_url not in priority_links:
                    priority_links.append(full_url)
            
            # Priority 2: All PDF links (many menus are PDFs without "menu" in filename)
            elif href.lower().endswith('.pdf'):
                if full_url not in pdf_links:
                    pdf_links.append(full_url)
        
        # Return prioritized list: menu/order links first, then all PDFs
        return priority_links + pdf_links
    
    def _has_menu_content(self, html_content: str) -> bool:
        """Check if HTML content appears to contain menu items"""
        html_lower = html_content.lower()
        
        # Check for menu indicators
        menu_keywords = ['menu', 'appetizer', 'entree', 'dessert', 'drink', 'price', '$']
        has_keywords = any(keyword in html_lower for keyword in menu_keywords)
        
        # Check for price patterns
        has_prices = bool(re.search(r'\$?\d+\.?\d{0,2}', html_content))
        
        # Check for structured menu data
        has_structured_data = 'schema.org/menuitem' in html_lower or 'schema.org/menu' in html_lower
        
        # Need at least keywords + prices, or structured data
        return (has_keywords and has_prices) or has_structured_data
    
    def _find_menu_image(self, html_content: str, base_url: str) -> Optional[str]:
        """
        Find menu images in HTML content.
        Looks for images near menu-related text or with menu-related alt text.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find images with menu-related alt text or near menu text
        for img in soup.find_all('img'):
            src = img.get('src', '')
            alt = img.get('alt', '').lower()
            
            if not src:
                continue
            
            # Check alt text for menu keywords
            if any(keyword in alt for keyword in ['menu', 'dinner', 'lunch', 'brunch', 'breakfast']):
                full_url = urljoin(base_url, src)
                return full_url
            
            # Check if image is near menu-related text
            parent = img.find_parent()
            if parent:
                parent_text = parent.get_text().lower()
                if any(keyword in parent_text for keyword in ['menu', 'dinner menu', 'lunch menu']):
                    full_url = urljoin(base_url, src)
                    return full_url
        
        # Also check for large images that might be menus
        # (heuristic: images > 500px width/height are often menus)
        for img in soup.find_all('img'):
            src = img.get('src', '')
            width = img.get('width', '')
            height = img.get('height', '')
            
            if src and (width or height):
                try:
                    w = int(width) if width else 0
                    h = int(height) if height else 0
                    if w > 500 or h > 500:
                        # Check if it's near menu context
                        parent = img.find_parent()
                        if parent:
                            parent_text = parent.get_text().lower()
                            if 'menu' in parent_text or 'dinner' in parent_text or 'lunch' in parent_text:
                                full_url = urljoin(base_url, src)
                                return full_url
                except:
                    pass
        
        return None
