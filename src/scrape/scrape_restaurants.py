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
import math
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
import sys
from dataclasses import dataclass
from dotenv import load_dotenv
from tqdm.asyncio import tqdm
from shapely.geometry import Point, Polygon
from load_manhattan_boundary import load_manhattan_boundary

try:
    from google.maps import places_v1
    from google.type import latlng_pb2
    from google.geo.type import types as geo_types
except ImportError:
    print("Error: google-maps-places package not installed.")
    print("Install it with: pip install google-maps-places")
    exit(1)

# Configuration
GRID_SIZE = 250  # 100x100 = 10000 cells (finer granularity for better coverage)
SLEEP_TIME = 30  # seconds between grid cell searches

# Manhattan bounding box (for grid generation - will be filtered by polygon)
MANHATTAN_BOUNDS = {
    "lat_low": 40.7000,   # Southern tip of Manhattan
    "lat_high": 40.8800,  # Northern tip (Inwood)
    "lon_low": -74.0500,  # Western edge (Hudson River)
    "lon_high": -73.9000, # Eastern edge (East River)
}

# Manhattan polygon boundary - loaded from official NYC Open Data
# This will be loaded at runtime using load_manhattan_boundary()
# Falls back to approximate polygon if download fails
def _get_manhattan_polygon() -> Polygon:
    """Load Manhattan boundary polygon from official NYC Open Data or use fallback."""
    global MANHATTAN_POLYGON
    
    if MANHATTAN_POLYGON is not None:
        return MANHATTAN_POLYGON
    
    # Try to load from official source
    try:
        print("Loading Manhattan boundary from NYC Open Data...")
        polygon = load_manhattan_boundary()
        if polygon is not None:
            MANHATTAN_POLYGON = polygon
            print(f"✅ Loaded official Manhattan boundary ({len(polygon.exterior.coords)} points)")
            return polygon
        else:
            print("⚠️  Could not load official boundary, using approximate polygon.")
    except Exception as e:
        print(f"⚠️  Failed to load official Manhattan boundary: {e}")
        print("Falling back to approximate polygon.")
    return MANHATTAN_POLYGON

# Initialize as None, will be loaded on first access
MANHATTAN_POLYGON: Optional[Polygon] = None
MAX_REVIEWS = 10
DETAILS_DELAY = 0.1  # seconds between Place Details calls
CACHE_GRID_DIR = Path("cache/grid")
CACHE_DETAILS_DIR = Path("cache/details")
OUTPUT_FILE = "manhattan_restaurants.json"
# Concurrency limits to avoid hitting rate limits
MAX_CONCURRENT_GRID_SEARCHES = 10  # Concurrent grid cell searches
MAX_CONCURRENT_DETAILS = 20  # Concurrent Place Details requests

# Field mask for searchNearby (nextPageToken is not a valid field path, it's returned automatically)
NEARBY_SEARCH_FIELDS = "places.id,places.displayName,places.formattedAddress,places.location,places.types,places.rating,places.userRatingCount,places.priceLevel,places.websiteUri,places.nationalPhoneNumber,places.businessStatus,places.reviewSummary"
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


def point_in_manhattan(lat: float, lon: float) -> bool:
    """
    Check if a point (latitude, longitude) is within Manhattan polygon.
    Uses shapely for accurate polygon intersection.
    """
    polygon = _get_manhattan_polygon()
    point = Point(lon, lat)  # Shapely uses (x, y) = (lon, lat)
    return polygon.contains(point) or polygon.touches(point)


def cell_overlaps_manhattan(cell: GridCell) -> bool:
    """
    Check if a grid cell overlaps with Manhattan polygon.
    A cell overlaps if any of its corners or center is within Manhattan.
    """
    # Check cell center
    center_lat = (cell.lat_low + cell.lat_high) / 2
    center_lon = (cell.lon_low + cell.lon_high) / 2
    if point_in_manhattan(center_lat, center_lon):
        return True
    
    # Check all four corners
    corners = [
        (cell.lat_low, cell.lon_low),   # SW corner
        (cell.lat_low, cell.lon_high),  # SE corner
        (cell.lat_high, cell.lon_low),  # NW corner
        (cell.lat_high, cell.lon_high), # NE corner
    ]
    
    for lat, lon in corners:
        if point_in_manhattan(lat, lon):
            return True
    
    # Also check if any edge of the cell intersects with Manhattan polygon
    # Create a polygon for the cell
    cell_polygon = Polygon([
        (cell.lon_low, cell.lat_low),   # SW
        (cell.lon_high, cell.lat_low),  # SE
        (cell.lon_high, cell.lat_high), # NE
        (cell.lon_low, cell.lat_high),  # NW
        (cell.lon_low, cell.lat_low),   # Close polygon
    ])
    
    # Check if cell polygon intersects Manhattan polygon
    polygon = _get_manhattan_polygon()
    return cell_polygon.intersects(polygon)


def generate_grid_cells() -> List[GridCell]:
    """
    Generate grid cells covering Manhattan with finer granularity.
    Only includes cells that overlap with Manhattan bounds to avoid
    querying areas outside Manhattan (like New Jersey, other boroughs).
    """
    all_cells = []
    lat_step = (MANHATTAN_BOUNDS["lat_high"] - MANHATTAN_BOUNDS["lat_low"]) / GRID_SIZE
    lon_step = (MANHATTAN_BOUNDS["lon_high"] - MANHATTAN_BOUNDS["lon_low"]) / GRID_SIZE

    # Generate all possible cells in the grid
    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            lat_low = MANHATTAN_BOUNDS["lat_low"] + i * lat_step
            lat_high = MANHATTAN_BOUNDS["lat_low"] + (i + 1) * lat_step
            lon_low = MANHATTAN_BOUNDS["lon_low"] + j * lon_step
            lon_high = MANHATTAN_BOUNDS["lon_low"] + (j + 1) * lon_step

            cell = GridCell(
                lat_low=lat_low,
                lon_low=lon_low,
                lat_high=lat_high,
                lon_high=lon_high
            )
            
            # Only include cells that overlap with Manhattan
            if cell_overlaps_manhattan(cell):
                all_cells.append(cell)

    print(f"Generated {len(all_cells)} cells overlapping Manhattan (out of {GRID_SIZE * GRID_SIZE} total grid cells)")
    return all_cells


def load_grid_cache(cell: GridCell) -> Optional[List[Dict]]:
    """Load grid cell results from cache if exists.
    Returns empty list [] if cache exists but is empty (to avoid re-querying).
    Returns None only if cache file doesn't exist.
    """
    cache_file = CACHE_GRID_DIR / cell.cache_filename()
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
                # Return cached data (even if empty list) to indicate cache exists
                return cached_data if isinstance(cached_data, list) else []
        except (json.JSONDecodeError, IOError):
            # If cache file is corrupted, treat as not cached
            return None
    return None


def save_grid_cache(cell: GridCell, results: List[Dict]):
    """Save grid cell results to cache, even if empty.
    Empty results are cached to avoid unnecessary API calls.
    """
    cache_file = CACHE_GRID_DIR / cell.cache_filename()
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


def load_details_cache(place_id: str) -> Optional[Dict]:
    """Load place details from cache if exists.
    Returns empty dict {} if cache exists but is empty (to avoid re-querying).
    Returns None only if cache file doesn't exist.
    """
    cache_file = CACHE_DETAILS_DIR / f"{place_id}.json"
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
                # Check for empty marker
                if cached_data == {"_empty": True}:
                    return {}
                # Return cached data (even if empty dict) to indicate cache exists
                return cached_data if isinstance(cached_data, dict) else {}
        except (json.JSONDecodeError, IOError):
            # If cache file is corrupted, treat as not cached
            return None
    return None


def save_details_cache(place_id: str, details: Optional[Dict]):
    """Save place details to cache, even if None or empty.
    Empty/None results are cached to avoid unnecessary API calls.
    """
    cache_file = CACHE_DETAILS_DIR / f"{place_id}.json"
    # Save empty dict with marker if details is None
    data_to_save = details if details is not None else {"_empty": True}
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(data_to_save, f, indent=2, ensure_ascii=False)


def create_client(api_key: str) -> places_v1.PlacesClient:
    """Create and configure Places API client with API key."""
    client_options = {"api_key": api_key}
    return places_v1.PlacesClient(client_options=client_options)


def create_async_client(api_key: str) -> places_v1.PlacesAsyncClient:
    """Create and configure Places API async client with API key."""
    client_options = {"api_key": api_key}
    return places_v1.PlacesAsyncClient(client_options=client_options)


def _convert_place_to_dict(place) -> Dict:
    """Convert a Place protobuf object to a dictionary."""
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
    return place_dict


async def search_by_type_async(
    client: places_v1.PlacesAsyncClient,
    included_types: List[str],
    cell: GridCell
) -> List[Dict]:
    """
    Search by place types (tags) asynchronously with pagination support.
    Uses SearchNearbyRequest with includedTypes for type-based filtering.
    Google Places API returns 20 results per page, so we need to paginate
    to get all results (up to 60 total).
    """
    all_results = []
    next_page_token = None
    page_num = 1
    max_pages = 3  # API allows up to 3 pages (60 results total)

    try:
        # Calculate center point of the cell
        center_lat = (cell.lat_low + cell.lat_high) / 2
        center_lon = (cell.lon_low + cell.lon_high) / 2
        
        center_point = latlng_pb2.LatLng(
            latitude=center_lat,
            longitude=center_lon
        )
        
        # Calculate radius to cover the entire cell and ensure overlap with adjacent cells
        # Radius must be >= distance from cell center to farthest corner (diagonal/2)
        # AND >= distance to adjacent cell centers to ensure full coverage overlap
        lat_diff = cell.lat_high - cell.lat_low
        lon_diff = cell.lon_high - cell.lon_low
        # Convert to meters: 1 degree latitude ≈ 111km, 1 degree longitude ≈ 111km * cos(latitude)
        lat_meters = lat_diff * 111000
        lon_meters = lon_diff * 111000 * math.cos(math.radians(center_lat))
        # Use diagonal distance (covers entire cell) plus buffer for overlap
        # This ensures radius >= distance to adjacent cell centers (diagonal distance)
        cell_diagonal = math.sqrt(lat_meters**2 + lon_meters**2)
        radius_meters = int(cell_diagonal + 100)  # 100m buffer ensures overlap
        
        # Cap radius at 50km (API limit for nearby search)
        # Note: For 20x20 grid, cell diagonal ≈ 1181m, so radius ≈ 1281m (well below limit)
        radius_meters = min(radius_meters, 50000)

        while page_num <= max_pages:
            # Build request
            if next_page_token:
                # Subsequent pages use page_token
                request = places_v1.SearchNearbyRequest(
                    page_token=next_page_token,
                    max_result_count=20
                )
            else:
                # First page uses location, radius, and included types
                location_restriction = places_v1.SearchNearbyRequest.LocationRestriction(
                    circle=places_v1.Circle(
                        center=center_point,
                        radius=radius_meters
                    )
                )
                request = places_v1.SearchNearbyRequest(
                    location_restriction=location_restriction,
                    included_types=included_types,
                    max_result_count=20
                )

            # Make async request with field mask (nextPageToken is returned automatically, not in field mask)
            response = await client.search_nearby(
                request=request,
                metadata=[("x-goog-fieldmask", NEARBY_SEARCH_FIELDS)]
            )

            # Convert protobuf response to dict and filter to only restaurants with websiteUri
            page_results = []
            if hasattr(response, 'places') and response.places:
                for place in response.places:
                    place_dict = _convert_place_to_dict(place)
                    # Only include restaurants that have a websiteUri
                    if place_dict.get('websiteUri') or place_dict.get('website_uri'):
                        page_results.append(place_dict)
            
            all_results.extend(page_results)
            
            # Check for next page token (try both snake_case and camelCase attribute names)
            next_page_token = None
            if hasattr(response, 'next_page_token') and response.next_page_token:
                next_page_token = response.next_page_token
            elif hasattr(response, 'nextPageToken') and response.nextPageToken:
                next_page_token = response.nextPageToken
            
            if next_page_token:
                page_num += 1
                # Small delay before next page request (API requirement)
                await asyncio.sleep(2)
            else:
                # No more pages - log if we might have hit a limit
                if len(page_results) == 20 and page_num < max_pages:
                    print(f"  Cell {cell.cache_filename()}: Got exactly 20 results on page {page_num} but no next_page_token")
                break

    except Exception as e:
        error_str = str(e)
        if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
            print(f"\nRate limit hit, waiting {SLEEP_TIME} seconds...")
            await asyncio.sleep(SLEEP_TIME)
        else:
            print(f"\nError in search_by_type_async for cell {cell.cache_filename()}: {e}")

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
            print(f"\nRate limit hit for place {place_id}, waiting {SLEEP_TIME} seconds...")
            await asyncio.sleep(SLEEP_TIME)
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
        # Check cache (returns None only if file doesn't exist, [] if empty cache exists)
        cached_results = load_grid_cache(cell)
        if cached_results is not None:
            # Cache exists (even if empty), return cached results
            return cached_results, True, False
        
        # Search for restaurants by type (more comprehensive than text query)
        # Include multiple restaurant-related types to catch all variations
        restaurant_types = [
            "restaurant",
        ]
        results = await search_by_type_async(
            client=client,
            included_types=restaurant_types,
            cell=cell
        )

        # Log how many results we got
        if results:
            limit_msg = " (hit 60 limit)" if len(results) >= 60 else ""
            print(f"  Cell {cell.cache_filename()}: Got {len(results)} results from API{limit_msg}")
        else:
            print(f"  Cell {cell.cache_filename()}: No results from API")

        # Always save to cache, even if empty (to avoid re-querying)
        # Deduplication will happen later when aggregating all results
        save_grid_cache(cell, results)
        return results, False, True


async def scrape_grid_cells_async(client: places_v1.PlacesAsyncClient) -> Dict[str, Dict]:
    """Phase 1: Scrape restaurants using grid strategy with parallelization."""
    print("Phase 1: Grid-based Text Search (Parallelized)")
    print("=" * 50)

    cells = generate_grid_cells()
    all_restaurants = {}
    cache_hits = 0
    api_calls = 0
    limit_hits = 0
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
        if len(results) >= 60:
            limit_hits += 1
        results_list.append(results)

    # Aggregate results and filter to only Manhattan restaurants using polygon check
    # Note: Results are already filtered to only include restaurants with websiteUri
    for results in results_list:
        for result in results:
            if 'id' in result:
                # Double-check websiteUri exists (should already be filtered, but verify)
                if result.get('websiteUri') or result.get('website_uri'):
                    all_restaurants[result['id']] = result

    print(f"\nGrid search complete:")
    print(f"  Cache hits: {cache_hits}")
    print(f"  API calls: {api_calls}")
    print(f"  Cells hitting 60 result limit: {limit_hits}")
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
        # Check cache (returns None only if file doesn't exist, {} if empty cache exists)
        cached_details = load_details_cache(place_id)
        if cached_details is not None:
            # Cache exists (even if empty), use cached details
            if cached_details:  # Only update if not empty
                restaurant.update(cached_details)
            return True, False
        
        # Fetch details
        details = await get_place_details_async(client, place_id)
        # Always save to cache, even if None (to avoid re-querying)
        save_details_cache(place_id, details)
        if details:
            restaurant.update(details)
            # Small delay to avoid rate limits
            await asyncio.sleep(DETAILS_DELAY)
            return False, True
        
        # No details found, but cached empty result
        return False, True


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
