# Manhattan Restaurants Scraper

A Python script that uses a grid strategy to query the Google Places API and collect all restaurants in Manhattan, including reviews and AI-generated summaries.

## Features

- **Grid-based search**: Divides Manhattan into fine-grained grid cells to overcome Google's 60-result limit
- **Incremental caching**: Saves results per grid cell and per restaurant for resumable runs
- **Duplicate minimization**: Location-based filtering to prevent duplicates between grid cells
- **Rich data**: Retrieves restaurant details, 10 most recent reviews, and AI-generated summaries
- **Progress tracking**: Shows progress bars and detailed logging

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up your Google Places API key:
```bash
cp .env.example .env
# Edit .env and add your API key
```

3. Get your API key from [Google Cloud Console](https://console.cloud.google.com/google/maps-apis)

## Usage

```bash
export GOOGLE_PLACES_API_KEY="your-api-key"
python scrape_restaurants.py
```

Or use the `.env` file:
```bash
python scrape_restaurants.py
```

## Output

The script generates:
- `manhattan_restaurants.json` - Final deduplicated JSON file with all restaurant data
- `cache/grid/` - Cached grid cell search results (for resuming)
- `cache/details/` - Cached place details (for resuming)

## Configuration

Edit constants in `scrape_restaurants.py`:
- `GRID_SIZE`: Number of grid cells (default: 20x20 = 400 cells)
- `MAX_REVIEWS`: Maximum reviews per restaurant (default: 10)
- `DETAILS_DELAY`: Delay between Place Details calls in seconds (default: 0.1)

## Notes

- The script uses caching to avoid redundant API calls. Delete `cache/` directory to start fresh.
- Place Details calls require Enterprise + Atmosphere SKU for reviews and AI summaries.
- The script respects rate limits with configurable delays and automatic retries.
- Uses the official `google-maps-places` Python SDK for type safety and better error handling.
