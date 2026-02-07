"""
Parser for PDF menus using OpenAI Vision API.
"""
import base64
import io
from typing import List, Optional
from pdf2image import convert_from_bytes
from PIL import Image
from ...models import MenuItem, Menu
from .base import BaseParser
import instructor
from openai import OpenAI
import httpx
import os
from dotenv import load_dotenv

load_dotenv()


class PdfParser(BaseParser):
    """Parse PDF menus using OpenAI Vision API"""
    
    def __init__(self):
        self.client = instructor.from_openai(OpenAI(api_key=os.getenv('OPENAI_API_KEY')))
    
    async def parse(self, url: str, html_content: Optional[str] = None) -> List[MenuItem]:
        """
        Parse menu from PDF using Vision API.
        
        Args:
            url: PDF URL
            html_content: Not used for PDF parsing
            
        Returns:
            List of MenuItem objects
        """
        try:
            # Download PDF
            pdf_bytes = await self._download_pdf(url)
            if not pdf_bytes:
                return []
            
            # Convert PDF to images
            images = convert_from_bytes(pdf_bytes)
            
            # Extract menu items from each page
            all_items = []
            for img in images:
                items = await self._extract_from_image(img)
                all_items.extend(items)
            
            return all_items
            
        except Exception as e:
            print(f"Error in PDF parsing for {url}: {e}")
            return []
    
    def can_parse(self, url: str, html_content: Optional[str] = None) -> bool:
        """Check if URL is a PDF"""
        return url.lower().endswith('.pdf') or '.pdf' in url.lower()
    
    async def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download PDF file"""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, follow_redirects=True)
                response.raise_for_status()
                return response.content
        except Exception as e:
            print(f"Error downloading PDF {url}: {e}")
            return None
    
    async def _extract_from_image(self, image: Image.Image) -> List[MenuItem]:
        """Extract menu items from image using Vision API"""
        try:
            # Convert image to base64
            buffered = io.BytesIO()
            image.save(buffered, format="PNG")
            img_base64 = base64.b64encode(buffered.getvalue()).decode()
            
            prompt = """Extract all menu items from this menu PDF page.

Rules:
- Identify appetizers (starters, small plates), entrees (main dishes), and drinks (beverages)
- Extract item names, prices (as numbers, e.g., 12.99 not "$12.99"), descriptions
- Generate relevant tags from descriptions
- Extract section names if available (e.g., "Appetizers", "Entrees", "Desserts")
- Return structured JSON matching the schema
"""
            
            menu = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_base64}"}}
                    ]
                }],
                response_model=Menu,
                temperature=0
            )
            
            return menu.items
            
        except Exception as e:
            print(f"Error extracting from image: {e}")
            return []
