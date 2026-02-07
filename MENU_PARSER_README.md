# Menu Parser

A modular menu parsing system that extracts restaurant menus from various formats and platforms.

## Architecture

The parser uses a modular architecture with clear separation of concerns:

### Core Components

- **`models.py`**: Pydantic models for menu items and restaurant data
- **`detection.py`**: Format and platform detection
- **`normalization.py`**: Data normalization layer
- **`cache.py`**: Caching logic for menu data
- **`parse_menus.py`**: Main orchestrator script

### Parsers (`menu_parsers/`)

- **`extruct_parser.py`**: Extracts structured data (JSON-LD, microdata, RDFa) from HTML
- **`html_llm_parser.py`**: Generic HTML parsing using LLM when structured data unavailable
- **`pdf_parser.py`**: PDF menu extraction using OpenAI Vision API
- **`toast_parser.py`**: Toast platform-specific parser
- **`doordash_parser.py`**: DoorDash/order.online platform parser

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Install Playwright browsers:
```bash
playwright install chromium
```

3. Set up environment variables:
```bash
cp .env.example .env
# Add OPENAI_API_KEY to .env
```

## Usage

### Basic Usage

```bash
python parse_menus.py --input manhattan_restaurants.json --output output/restaurant_menus.json
```

### Options

- `--input`: Input JSON file with restaurants (default: `manhattan_restaurants.json`)
- `--output`: Output JSON file for menus (default: `output/restaurant_menus.json`)
- `--max-concurrent`: Maximum concurrent requests (default: 10)

## Parsing Strategy

The parser uses a three-path strategy:

1. **extruct (HTML)**: Extracts structured data from HTML - fast, reliable, free
2. **OpenAI Vision API (PDF)**: Extracts menu items from PDF files
3. **Platform Scraping**: Platform-specific parsers for Toast, DoorDash, etc.

### Decision Flow

```
Restaurant Website
    ↓
PDF? → OpenAI Vision API ✅
    ↓
Toast/Online Ordering? → Platform Scraper ✅
    ↓
Otherwise → extruct (HTML) → Fallback to HTML+LLM ✅
```

## Output Format

```json
{
  "restaurant_id": "ChIJ...",
  "restaurant_name": "Hav & Mar",
  "website_uri": "https://www.havandmar.com/menus/#lunch",
  "platform_detected": "html",
  "extraction_date": "2026-02-06T12:00:00Z",
  "confidence_score": 0.95,
  "menu": [
    {
      "name": "Blue Cornbread",
      "price": 13.00,
      "type": "appetizer",
      "section": "Appetizers",
      "description": "Warm blue masa cornbread...",
      "tags": ["bread", "vegetarian"],
      "dietary_info": ["vegetarian"]
    }
  ]
}
```

## Caching

Menus are cached in `cache/menus/` directory. Cache is valid for 30 days by default.

## Error Handling

The parser includes comprehensive error handling:
- Failed requests are logged but don't stop processing
- Partial results are saved
- Cache prevents re-parsing successful extractions

## Extending

To add a new parser:

1. Create a new parser class in `menu_parsers/` inheriting from `BaseParser`
2. Implement `parse()` and `can_parse()` methods
3. Add parser to `MenuParser.parsers` dictionary in `parse_menus.py`
