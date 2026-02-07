"""
Load Manhattan boundary polygon from official NYC Open Data.
Downloads and caches the GeoJSON data, then extracts Manhattan's polygon.
"""
import json
import httpx
from pathlib import Path
from shapely.geometry import shape, Polygon
from typing import Optional

# Cache directory for boundary data
BOUNDARY_CACHE_DIR = Path("cache/boundaries")
BOUNDARY_CACHE_FILE = BOUNDARY_CACHE_DIR / "manhattan_boundary.json"

# NYC Open Data API endpoint for borough boundaries
NYC_BOROUGH_BOUNDARIES_URL = "https://data.cityofnewyork.us/api/geospatial/tqmj-j8zm?method=export&format=GeoJSON"

# Alternative: GitHub mirror (more reliable)
GITHUB_BOROUGHS_URL = "https://raw.githubusercontent.com/dwillis/nyc-maps/master/boroughs.geojson"


def download_borough_boundaries() -> Optional[dict]:
    """Download borough boundaries GeoJSON from NYC Open Data."""
    try:
        # Try GitHub mirror first (more reliable)
        with httpx.Client(timeout=30.0) as client:
            response = client.get(GITHUB_BOROUGHS_URL)
            response.raise_for_status()
            return response.json()
    except Exception as e:
        print(f"Error downloading from GitHub: {e}")
        try:
            # Fallback to NYC Open Data
            with httpx.Client(timeout=30.0) as client:
                response = client.get(NYC_BOROUGH_BOUNDARIES_URL)
                response.raise_for_status()
                return response.json()
        except Exception as e2:
            print(f"Error downloading from NYC Open Data: {e2}")
            return None


def extract_manhattan_polygon(geojson_data: dict) -> Optional[Polygon]:
    """Extract Manhattan polygon from GeoJSON data."""
    if geojson_data.get('type') != 'FeatureCollection':
        print("Unexpected GeoJSON format")
        return None
    
    # Find Manhattan feature
    for feature in geojson_data.get('features', []):
        properties = feature.get('properties', {})
        # Check various possible property names (case-insensitive)
        borough_name = (properties.get('boro_name', '') or properties.get('BoroName', '')).upper()
        boro_code = properties.get('boro_code') or properties.get('BoroCode')
        
        # Check by name or code (Manhattan is code 1)
        if borough_name in ['MANHATTAN', 'MN'] or boro_code == 1:
            geometry = feature.get('geometry')
            if geometry:
                geom_type = geometry.get('type')
                # Handle both Polygon and MultiPolygon
                try:
                    shapely_geom = shape(geometry)
                    if isinstance(shapely_geom, Polygon):
                        return shapely_geom
                    elif hasattr(shapely_geom, 'geoms'):  # MultiPolygon
                        # For MultiPolygon, return the largest polygon (main island)
                        largest = max(shapely_geom.geoms, key=lambda p: p.area)
                        return largest
                except Exception as e:
                    print(f"Error converting geometry: {e}")
                    continue
    
    # If not found by name, try to find by coordinates (Manhattan is roughly centered)
    print("Manhattan not found by name, searching by coordinates...")
    for feature in geojson_data.get('features', []):
        geometry = feature.get('geometry')
        if geometry and geometry.get('type') == 'Polygon':
            try:
                shapely_geom = shape(geometry)
                if isinstance(shapely_geom, Polygon):
                    # Check if centroid is in Manhattan area
                    centroid = shapely_geom.centroid
                    if (40.75 <= centroid.y <= 40.85 and -74.02 <= centroid.x <= -73.93):
                        return shapely_geom
            except Exception as e:
                continue
    
    return None


def load_manhattan_boundary(force_download: bool = False) -> Optional[Polygon]:
    """
    Load Manhattan boundary polygon.
    
    Args:
        force_download: If True, download fresh data even if cache exists
        
    Returns:
        Shapely Polygon object representing Manhattan's boundary, or None if failed
    """
    # Check cache first
    if not force_download and BOUNDARY_CACHE_FILE.exists():
        try:
            with open(BOUNDARY_CACHE_FILE, 'r') as f:
                cached_data = json.load(f)
                # Reconstruct polygon from cached coordinates
                coords = cached_data.get('coordinates', [])
                if coords:
                    return Polygon(coords)
        except Exception as e:
            print(f"Error loading cached boundary: {e}")
    
    # Download fresh data
    print("Downloading Manhattan boundary from NYC Open Data...")
    geojson_data = download_borough_boundaries()
    
    if not geojson_data:
        print("Failed to download boundary data")
        return None
    
    # Extract Manhattan polygon
    polygon = extract_manhattan_polygon(geojson_data)
    
    if polygon:
        # Cache the polygon coordinates
        BOUNDARY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(BOUNDARY_CACHE_FILE, 'w') as f:
            json.dump({
                'coordinates': list(polygon.exterior.coords),
                'source': 'NYC Open Data'
            }, f, indent=2)
        print(f"✅ Loaded Manhattan boundary ({len(polygon.exterior.coords)} points)")
        return polygon
    else:
        print("❌ Could not extract Manhattan polygon from GeoJSON")
        return None


if __name__ == "__main__":
    # Test loading
    polygon = load_manhattan_boundary()
    if polygon:
        print(f"Manhattan polygon loaded: {len(polygon.exterior.coords)} points")
        print(f"Bounds: {polygon.bounds}")
