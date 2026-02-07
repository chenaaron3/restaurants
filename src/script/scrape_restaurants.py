#!/usr/bin/env python3
"""
Manhattan Restaurants Scraper

Uses a grid strategy to query Google Places API and collect all restaurants
in Manhattan, including reviews and AI-generated summaries.
"""

import os
import json
import time
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from dotenv import load_dotenv
from tqdm.asyncio import tqdm

try:
    from google.maps import places_v1
    from google.type import latlng_pb2
    from google.geo.type import types as geo_types
except ImportError:
    print("Error: google-maps-places package not installed.")
    print("Install it with: pip install google-maps-places")
    exit(1)

# Configuration
GRID_SIZE = 20  # 20x20 = 400 cells
MANHATTAN_BOUNDS = {
    "lat_low": 40.7000,
    "lat_high": 40.8800,
    "lon_low": -74.0500,
    "lon_high": -73.9000,
}
MAX_REVIEWS = 10
DETAILS_DELAY = 0.1  # seconds between Place Details calls
CACHE_GRID_DIR = Path("cache/grid")
CACHE_DETAILS_DIR = Path("cache/details")
OUTPUT_FILE = "manhattan_restaurants.json"
# Concurrency limits to avoid hitting rate limits
MAX_CONCURRENT_GRID_SEARCHES = 10  # Concurrent grid cell searches
MAX_CONCURRENT_DETAILS = 20  # Concurrent Place Details requests

# Field masks
TEXT_SEARCH_FIELDS = "places.id,places.displayName,places.formattedAddress,places.location,places.types,places.rating,places.userRatingCount,places.priceLevel,places.websiteUri,places.nationalPhoneNumber,places.businessStatus,places.reviewSummary,nextPageToken"

PLACE_DETAILS_FIELDS = "id,displayName,reviews,generativeSummary,reviewSummary"


@dataclass
class GridCell:
    """Represents a grid cell with bounding coordinates."""
    lat_low: float
    lon_low: float
    lat_high: float
    lon_high: float

    def cache_filename(self) -> str:
        """Generate cache filename from cell coordinates."""
        return f"{self.lat_low}_{self.lon_low}_{self.lat_high}_{self.lon_high}.json"


def load_env() -> str:
    """Load environment variables and return API key."""
    load_dotenv()
    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    if not api_key:
        raise ValueError(
            "GOOGLE_PLACES_API_KEY not found in environment variables. "
            "Set it in .env file or export it."
        )
    return api_key


def create_directories():
    """Create cache directories if they don't exist."""
    CACHE_GRID_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DETAILS_DIR.mkdir(parents=True, exist_ok=True)


def generate_grid_cells() -> List[GridCell]:
    """Generate grid cells covering Manhattan."""
    cells = []
    lat_step = (MANHATTAN_BOUNDS["lat_high"] - MANHATTAN_BOUNDS["lat_low"]) / GRID_SIZE
    lon_step = (MANHATTAN_BOUNDS["lon_high"] - MANHATTAN_BOUNDS["lon_low"]) / GRID_SIZE

    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            lat_low = MANHATTAN_BOUNDS["lat_low"] + i * lat_step
            lat_high = MANHATTAN_BOUNDS["lat_low"] + (i + 1) * lat_step
            lon_low = MANHATTAN_BOUNDS["lon_low"] + j * lon_step
            lon_high = MANHATTAN_BOUNDS["lon_low"] + (j + 1) * lon_step

            cells.append(GridCell(
                lat_low=lat_low,
                lon_low=lon_low,
                lat_high=lat_high,
                lon_high=lon_high
            ))

    return cells


def is_in_cell(lat: float, lon: float, cell: GridCell) -> bool:
    """Check if coordinates are within cell bounds."""
    return (cell.lat_low <= lat < cell.lat_high and
            cell.lon_low <= lon < cell.lon_high)


def load_grid_cache(cell: GridCell) -> Optional[List[Dict]]:
    """Load grid cell results from cache if exists."""
    cache_file = CACHE_GRID_DIR / cell.cache_filename()
    if cache_file.exists():
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def save_grid_cache(cell: GridCell, results: List[Dict]):
    """Save grid cell results to cache."""
    cache_file = CACHE_GRID_DIR / cell.cache_filename()
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


def load_details_cache(place_id: str) -> Optional[Dict]:
    """Load place details from cache if exists."""
    cache_file = CACHE_DETAILS_DIR / f"{place_id}.json"
    if cache_file.exists():
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def save_details_cache(place_id: str, details: Dict):
    """Save place details to cache."""
    cache_file = CACHE_DETAILS_DIR / f"{place_id}.json"
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(details, f, indent=2, ensure_ascii=False)


def create_client(api_key: str) -> places_v1.PlacesClient:
    """Create and configure Places API client with API key."""
    client_options = {"api_key": api_key}
    return places_v1.PlacesClient(client_options=client_options)


def create_async_client(api_key: str) -> places_v1.PlacesAsyncClient:
    """Create and configure Places API async client with API key."""
    client_options = {"api_key": api_key}
    return places_v1.PlacesAsyncClient(client_options=client_options)


async def search_text_async(
    client: places_v1.PlacesAsyncClient,
    text_query: str,
    cell: GridCell
) -> List[Dict]:
    """Search text asynchronously - try to get all 60 results in one request."""
    all_results = []

    try:
        # Create viewport (rectangle) for location restriction
        low_point = latlng_pb2.LatLng(
            latitude=cell.lat_low,
            longitude=cell.lon_low
        )
        high_point = latlng_pb2.LatLng(
            latitude=cell.lat_high,
            longitude=cell.lon_high
        )
        
        viewport = geo_types.Viewport(
            low=low_point,
            high=high_point
        )
        
        location_restriction = places_v1.SearchTextRequest.LocationRestriction(
            rectangle=viewport
        )

        # Build request - try max_result_count=60 to get all results in one go
        # The API may cap this at 20 per page, but worth trying
        request = places_v1.SearchTextRequest(
            text_query=text_query,
            location_restriction=location_restriction,
            max_result_count=60  # Try to get all 60 results in one request
        )

        # Make async request with field mask
        response = await client.search_text(
            request=request,
            metadata=[("x-goog-fieldmask", TEXT_SEARCH_FIELDS)]
        )

        # Convert protobuf response to dict
        if hasattr(response, 'places') and response.places:
            for place in response.places:
                place_dict = {}
                if hasattr(place, 'id'):
                    place_dict['id'] = place.id
                if hasattr(place, 'display_name') and place.display_name:
                    place_dict['displayName'] = {
                        'text': place.display_name.text,
                        'languageCode': place.display_name.language_code
                    }
                if hasattr(place, 'formatted_address'):
                    place_dict['formattedAddress'] = place.formatted_address
                if hasattr(place, 'location') and place.location:
                    place_dict['location'] = {
                        'latitude': place.location.latitude,
                        'longitude': place.location.longitude
                    }
                if hasattr(place, 'types'):
                    place_dict['types'] = list(place.types)
                if hasattr(place, 'rating'):
                    place_dict['rating'] = place.rating
                if hasattr(place, 'user_rating_count'):
                    place_dict['userRatingCount'] = place.user_rating_count
                if hasattr(place, 'price_level'):
                    place_dict['priceLevel'] = place.price_level.name if place.price_level else None
                if hasattr(place, 'website_uri'):
                    place_dict['websiteUri'] = place.website_uri
                if hasattr(place, 'national_phone_number'):
                    place_dict['nationalPhoneNumber'] = place.national_phone_number
                if hasattr(place, 'business_status'):
                    place_dict['businessStatus'] = place.business_status.name if place.business_status else None
                if hasattr(place, 'review_summary') and place.review_summary:
                    rs = place.review_summary
                    place_dict['reviewSummary'] = {
                        'text': {
                            'text': rs.text.text if rs.text else '',
                            'languageCode': rs.text.language_code if rs.text else 'en'
                        } if rs.text else None,
                        'reviewsUri': rs.reviews_uri if hasattr(rs, 'reviews_uri') else None,
                        'disclosureText': {
                            'text': rs.disclosure_text.text if rs.disclosure_text else '',
                            'languageCode': rs.disclosure_text.language_code if rs.disclosure_text else 'en'
                        } if hasattr(rs, 'disclosure_text') and rs.disclosure_text else None
                    }
                all_results.append(place_dict)

    except Exception as e:
        error_str = str(e)
        if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
            print(f"\nRate limit hit, waiting 60 seconds...")
            await asyncio.sleep(60)
        else:
            print(f"\nError in search_text_async: {e}")

    return all_results


async def get_place_details_async(
    client: places_v1.PlacesAsyncClient,
    place_id: str
) -> Optional[Dict]:
    """Get place details including reviews and AI summaries."""
    try:
        # Build request
        request = places_v1.GetPlaceRequest(
            name=f"places/{place_id}"
        )

        # Make async request with field mask
        response = await client.get_place(
            request=request,
            metadata=[("x-goog-fieldmask", PLACE_DETAILS_FIELDS)]
        )

        details = {}

        # Extract reviews (limit to most recent MAX_REVIEWS)
        if hasattr(response, 'reviews') and response.reviews:
            reviews_list = []
            # Sort by publish time (most recent first) if available
            def get_publish_time(review):
                if hasattr(review, 'publish_time') and review.publish_time:
                    # Convert protobuf timestamp to sortable string
                    try:
                        return review.publish_time.seconds if hasattr(review.publish_time, 'seconds') else 0
                    except:
                        return 0
                return 0
            
            sorted_reviews = sorted(
                response.reviews,
                key=get_publish_time,
                reverse=True
            )
            for review in sorted_reviews[:MAX_REVIEWS]:
                review_dict = {}
                if hasattr(review, 'rating'):
                    review_dict['rating'] = review.rating
                if hasattr(review, 'text') and review.text:
                    review_dict['text'] = {
                        'text': review.text.text,
                        'languageCode': review.text.language_code
                    }
                if hasattr(review, 'author_attribution') and review.author_attribution:
                    author = review.author_attribution
                    review_dict['authorAttribution'] = {
                        'displayName': author.display_name if hasattr(author, 'display_name') else None,
                        'uri': author.uri if hasattr(author, 'uri') else None,
                        'photoUri': author.photo_uri if hasattr(author, 'photo_uri') else None
                    }
                if hasattr(review, 'publish_time') and review.publish_time:
                    # Convert protobuf timestamp to ISO format string
                    try:
                        from google.protobuf.timestamp_pb2 import Timestamp
                        if isinstance(review.publish_time, Timestamp):
                            dt = datetime.fromtimestamp(review.publish_time.seconds + review.publish_time.nanos / 1e9)
                            review_dict['publishTime'] = dt.isoformat() + 'Z'
                        else:
                            review_dict['publishTime'] = str(review.publish_time)
                    except:
                        review_dict['publishTime'] = str(review.publish_time)
                reviews_list.append(review_dict)
            details['reviews'] = reviews_list

        # Extract generative summary
        if hasattr(response, 'generative_summary') and response.generative_summary:
            gen_summary = response.generative_summary
            details['generativeSummary'] = {}
            if hasattr(gen_summary, 'overview') and gen_summary.overview:
                details['generativeSummary']['overview'] = {
                    'text': gen_summary.overview.text,
                    'languageCode': gen_summary.overview.language_code
                }
            if hasattr(gen_summary, 'description') and gen_summary.description:
                details['generativeSummary']['description'] = {
                    'text': gen_summary.description.text,
                    'languageCode': gen_summary.description.language_code
                }

        # Extract review summary (if not already from Text Search)
        if hasattr(response, 'review_summary') and response.review_summary:
            rs = response.review_summary
            details['reviewSummary'] = {
                'text': {
                    'text': rs.text.text if rs.text else '',
                    'languageCode': rs.text.language_code if rs.text else 'en'
                } if rs.text else None,
                'reviewsUri': rs.reviews_uri if hasattr(rs, 'reviews_uri') else None,
                'disclosureText': {
                    'text': rs.disclosure_text.text if rs.disclosure_text else '',
                    'languageCode': rs.disclosure_text.language_code if rs.disclosure_text else 'en'
                } if hasattr(rs, 'disclosure_text') and rs.disclosure_text else None
            }

        return details if details else None

    except Exception as e:
        error_str = str(e)
        if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
            print(f"\nRate limit hit for place {place_id}, waiting 60 seconds...")
            await asyncio.sleep(60)
        else:
            print(f"\nError getting place details for {place_id}: {e}")
        return None


async def process_cell_async(
    client: places_v1.PlacesAsyncClient,
    cell: GridCell,
    semaphore: asyncio.Semaphore
) -> Tuple[List[Dict], bool, bool]:
    """Process a single grid cell asynchronously."""
    async with semaphore:
        # Check cache
        cached_results = load_grid_cache(cell)
        if cached_results:
            return cached_results, True, False
        
        # Search for restaurants
        results = await search_text_async(
            client=client,
            text_query="restaurant",
            cell=cell
        )

        # Filter by location
        filtered_results = []
        for result in results:
            if 'location' in result:
                lat = result['location']['latitude']
                lon = result['location']['longitude']
                if is_in_cell(lat, lon, cell):
                    filtered_results.append(result)

        # Save to cache
        save_grid_cache(cell, filtered_results)
        return filtered_results, False, True


async def scrape_grid_cells_async(client: places_v1.PlacesAsyncClient) -> Dict[str, Dict]:
    """Phase 1: Scrape restaurants using grid strategy with parallelization."""
    print("Phase 1: Grid-based Text Search (Parallelized)")
    print("=" * 50)

    cells = generate_grid_cells()
    all_restaurants = {}
    cache_hits = 0
    api_calls = 0
    filtered_count = 0

    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_GRID_SEARCHES)

    # Process all cells concurrently
    tasks = [
        process_cell_async(client, cell, semaphore)
        for cell in cells
    ]

    # Use tqdm for progress tracking
    results_list = []
    for coro in tqdm.as_completed(tasks, desc="Processing grid cells", total=len(tasks)):
        results, from_cache, made_api_call = await coro
        if from_cache:
            cache_hits += 1
        if made_api_call:
            api_calls += 1
        results_list.append(results)

    # Aggregate results
    for results in results_list:
        for result in results:
            if 'id' in result:
                all_restaurants[result['id']] = result

    print(f"\nGrid search complete:")
    print(f"  Cache hits: {cache_hits}")
    print(f"  API calls: {api_calls}")
    print(f"  Unique restaurants found: {len(all_restaurants)}")

    return all_restaurants


async def enrich_restaurant_async(
    client: places_v1.PlacesAsyncClient,
    place_id: str,
    restaurant: Dict,
    semaphore: asyncio.Semaphore
) -> Tuple[bool, bool]:
    """Enrich a single restaurant asynchronously."""
    async with semaphore:
        # Check cache
        cached_details = load_details_cache(place_id)
        if cached_details:
            restaurant.update(cached_details)
            return True, False
        
        # Fetch details
        details = await get_place_details_async(client, place_id)
        if details:
            save_details_cache(place_id, details)
            restaurant.update(details)
            # Small delay to avoid rate limits
            await asyncio.sleep(DETAILS_DELAY)
            return False, True
        
        return False, False


async def enrich_restaurants_async(
    client: places_v1.PlacesAsyncClient,
    restaurants: Dict[str, Dict]
):
    """Phase 2: Enrich restaurants with Place Details (parallelized)."""
    print("\nPhase 2: Place Details Enrichment (Parallelized)")
    print("=" * 50)

    cache_hits = 0
    api_calls = 0
    enriched_count = 0

    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DETAILS)

    # Process all restaurants concurrently
    tasks = [
        enrich_restaurant_async(client, place_id, restaurant, semaphore)
        for place_id, restaurant in restaurants.items()
    ]

    # Use tqdm for progress tracking
    for coro in tqdm.as_completed(tasks, desc="Enriching restaurants", total=len(tasks)):
        from_cache, made_api_call = await coro
        if from_cache:
            cache_hits += 1
        if made_api_call:
            api_calls += 1
            enriched_count += 1
        elif not from_cache:
            enriched_count += 1

    print(f"\nEnrichment complete:")
    print(f"  Cache hits: {cache_hits}")
    print(f"  API calls: {api_calls}")
    print(f"  Enriched restaurants: {enriched_count}")


def save_output(restaurants: Dict[str, Dict]):
    """Save final output to JSON file."""
    print(f"\nSaving output to {OUTPUT_FILE}...")

    output = {
        "total_count": len(restaurants),
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        "restaurants": list(restaurants.values())
    }

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(restaurants)} restaurants to {OUTPUT_FILE}")


async def main_async():
    """Main execution function (async)."""
    print("Manhattan Restaurants Scraper (Parallelized)")
    print("=" * 50)

    # Setup
    api_key = load_env()
    create_directories()
    client = create_async_client(api_key)

    # Phase 1: Grid search (parallelized)
    restaurants = await scrape_grid_cells_async(client)

    # Phase 2: Enrichment (parallelized)
    await enrich_restaurants_async(client, restaurants)

    # Save output
    save_output(restaurants)

    print("\nDone!")


def main():
    """Main entry point - runs async main."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
