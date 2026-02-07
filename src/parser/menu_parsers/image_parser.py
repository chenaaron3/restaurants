"""
Parser for image-based menus using OpenAI Vision API.
"""
import base64
import io
from typing import List, Optional
import httpx
from PIL import Image
from ...models import MenuItem, Menu
from .base import BaseParser
import instructor
from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()


class ImageParser(BaseParser):
    """Parse image menus using OpenAI Vision API"""
    
    def __init__(self):
        self.client = instructor.from_openai(OpenAI(api_key=os.getenv('OPENAI_API_KEY')))
    
    async def parse(self, url: str, html_content: Optional[str] = None) -> List[MenuItem]:
        """
        Parse menu from image using Vision API.
        
        Args:
            url: Image URL
            html_content: Not used for image parsing
            
        Returns:
            List of MenuItem objects
        """
        try:
            # Download image
            image = await self._download_image(url)
            if not image:
                return []
            
            # Extract menu items from image
            items = await self._extract_from_image(image)
            return items
            
        except Exception as e:
            print(f"Error in image parsing for {url}: {e}")
            return []
    
    def can_parse(self, url: str, html_content: Optional[str] = None) -> bool:
        """Check if URL is an image"""
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in image_extensions)
    
    async def _download_image(self, url: str) -> Optional[Image.Image]:
        """Download image file"""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, follow_redirects=True)
                response.raise_for_status()
                image_bytes = response.content
                return Image.open(io.BytesIO(image_bytes))
        except Exception as e:
            print(f"Error downloading image {url}: {e}")
            return None
    
    async def _extract_from_image(self, image: Image.Image) -> List[MenuItem]:
        """Extract menu items from image using Vision API"""
        try:
            # Convert image to base64
            buffered = io.BytesIO()
            # Convert to RGB if necessary (some images have transparency)
            if image.mode != 'RGB':
                image = image.convert('RGB')
            image.save(buffered, format="PNG")
            img_base64 = base64.b64encode(buffered.getvalue()).decode()
            
            prompt = """Extract all menu items from this menu image.

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
