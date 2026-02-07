"""
Parser factory for selecting the appropriate parser based on content.
"""
from typing import Optional
from .menu_parsers import ExtructParser, HtmlLlmParser, PdfParser, ImageParser
from .menu_parsers.base import BaseParser
import re


class ParserFactory:
    """Factory for creating appropriate parsers based on URL and content"""
    
    def __init__(self):
        self.extruct_parser = ExtructParser()
        self.html_llm_parser = HtmlLlmParser()
        self.pdf_parser = PdfParser()
        self.image_parser = ImageParser()
    
    def get_parser(self, url: str, html_content: Optional[str] = None) -> BaseParser:
        """
        Get the appropriate parser for the given URL/content.
        
        Args:
            url: Website URL
            html_content: Optional HTML content
            
        Returns:
            Appropriate parser instance
        """
        # Check for PDF first
        if self._is_pdf(url):
            return self.pdf_parser
        
        # Check for images
        if self._is_image(url):
            return self.image_parser
        
        # For HTML content, return extruct parser (it will fallback to html_llm internally)
        # The parse_menus.py will handle the fallback chain
        return self.extruct_parser
    
    def _is_pdf(self, url: str) -> bool:
        """Check if URL points to a PDF"""
        return url.lower().endswith('.pdf') or '.pdf' in url.lower()
    
    def _is_image(self, url: str) -> bool:
        """Check if URL points to an image"""
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in image_extensions)
