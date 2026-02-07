"""
Format and platform detection for menu extraction.
"""
import re
from typing import Optional, Tuple
from urllib.parse import urlparse
import httpx


class FormatDetector:
    """Detects menu format: PDF, Toast/Online Ordering, or HTML"""
    
    # Platform URL patterns
    TOAST_PATTERNS = [
        r'toasttab\.com',
        r'\.toasttab\.com',
    ]
    
    DOORDASH_PATTERNS = [
        r'doordash\.com/store/',
        r'order\.online/store/',
    ]
    
    RESY_PATTERNS = [
        r'resy\.com',
    ]
    
    OPENTABLE_PATTERNS = [
        r'opentable\.com',
    ]
    
    GRUBHUB_PATTERNS = [
        r'grubhub\.com/restaurant/',
    ]
    
    CHOWNOW_PATTERNS = [
        r'chownow\.com/order/',
    ]
    
    SQUARE_PATTERNS = [
        r'squareup\.com',
        r'square\.site',
    ]
    
    def __init__(self):
        self.toast_regex = re.compile('|'.join(self.TOAST_PATTERNS), re.IGNORECASE)
        self.doordash_regex = re.compile('|'.join(self.DOORDASH_PATTERNS), re.IGNORECASE)
        self.resy_regex = re.compile('|'.join(self.RESY_PATTERNS), re.IGNORECASE)
        self.opentable_regex = re.compile('|'.join(self.OPENTABLE_PATTERNS), re.IGNORECASE)
        self.grubhub_regex = re.compile('|'.join(self.GRUBHUB_PATTERNS), re.IGNORECASE)
        self.chownow_regex = re.compile('|'.join(self.CHOWNOW_PATTERNS), re.IGNORECASE)
        self.square_regex = re.compile('|'.join(self.SQUARE_PATTERNS), re.IGNORECASE)
    
    def detect_format(self, url: str, html_content: Optional[str] = None) -> Tuple[str, Optional[str]]:
        """
        Detect menu format and platform.
        
        Returns:
            Tuple of (format, platform)
            format: 'pdf', 'platform', or 'html'
            platform: platform name if detected (e.g., 'toast', 'doordash'), None otherwise
        """
        # Check for PDF links in URL or HTML
        if self._is_pdf_url(url) or (html_content and self._has_pdf_links(html_content)):
            return ('pdf', None)
        
        # Check for platform patterns
        platform = self.detect_platform(url, html_content)
        if platform:
            return ('platform', platform)
        
        # Default to HTML
        return ('html', None)
    
    def detect_platform(self, url: str, html_content: Optional[str] = None) -> Optional[str]:
        """Detect specific platform from URL or HTML"""
        if self.toast_regex.search(url) or (html_content and self._has_toast_indicators(html_content)):
            return 'toast'
        if self.doordash_regex.search(url) or (html_content and self._has_doordash_indicators(html_content)):
            return 'doordash'
        if self.resy_regex.search(url):
            return 'resy'
        if self.opentable_regex.search(url):
            return 'opentable'
        if self.grubhub_regex.search(url):
            return 'grubhub'
        if self.chownow_regex.search(url):
            return 'chownow'
        if self.square_regex.search(url):
            return 'square'
        return None
    
    def _is_pdf_url(self, url: str) -> bool:
        """Check if URL points to a PDF"""
        return url.lower().endswith('.pdf') or '.pdf' in url.lower()
    
    def _has_pdf_links(self, html_content: str) -> bool:
        """Check if HTML contains PDF links"""
        pdf_pattern = re.compile(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', re.IGNORECASE)
        return bool(pdf_pattern.search(html_content))
    
    def _has_toast_indicators(self, html_content: str) -> bool:
        """Check for Toast platform indicators in HTML"""
        indicators = [
            'window.toast',
            'toast-menu',
            'toasttab',
        ]
        html_lower = html_content.lower()
        return any(indicator.lower() in html_lower for indicator in indicators)
    
    def _has_doordash_indicators(self, html_content: str) -> bool:
        """Check for DoorDash platform indicators in HTML"""
        indicators = [
            'doordash',
            'doordash-menu',
        ]
        html_lower = html_content.lower()
        return any(indicator.lower() in html_lower for indicator in indicators)


async def fetch_html(url: str) -> Optional[str]:
    """Fetch HTML content from URL"""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()
            return response.text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None
