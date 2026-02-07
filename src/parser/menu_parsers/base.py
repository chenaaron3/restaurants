"""
Base parser class for menu extraction.
"""
from abc import ABC, abstractmethod
from typing import List, Optional
from ...models import MenuItem


class BaseParser(ABC):
    """Base class for all menu parsers"""
    
    @abstractmethod
    async def parse(self, url: str, html_content: Optional[str] = None) -> List[MenuItem]:
        """
        Parse menu from URL or HTML content.
        
        Args:
            url: Restaurant website URL
            html_content: Optional pre-fetched HTML content
            
        Returns:
            List of MenuItem objects
        """
        pass
    
    @abstractmethod
    def can_parse(self, url: str, html_content: Optional[str] = None) -> bool:
        """
        Check if this parser can handle the given URL/content.
        
        Args:
            url: Restaurant website URL
            html_content: Optional HTML content
            
        Returns:
            True if parser can handle this content
        """
        pass
