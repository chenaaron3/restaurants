#!/usr/bin/env python3
"""
Create a static map visualization of all scraped restaurant locations.
"""
import json
import sys
from pathlib import Path
from typing import List, Tuple, Optional
import folium
from folium.plugins import MarkerCluster
from playwright.sync_api import sync_playwright
import time

from scrape_restaurants import _get_manhattan_polygon

def load_restaurants(json_file: str) -> List[dict]:
    """Load restaurants from JSON file."""
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if isinstance(data, dict) and 'restaurants' in data:
        return data['restaurants']
    elif isinstance(data, list):
        return data
    else:
        raise ValueError("Unexpected JSON structure")

def extract_locations(restaurants: List[dict]) -> List[Tuple[float, float, str]]:
    """Extract (lat, lon, name) tuples from restaurants."""
    locations = []
    for restaurant in restaurants:
        if 'location' in restaurant and restaurant['location']:
            lat = restaurant['location'].get('latitude')
            lon = restaurant['location'].get('longitude')
            name = restaurant.get('displayName', {}).get('text', 'Unknown')
            
            if lat is not None and lon is not None:
                locations.append((lat, lon, name))
    
    return locations

def create_map(locations: List[Tuple[float, float, str]], output_html: str = "restaurants_map.html"):
    """Create an interactive Folium map."""
    if not locations:
        print("No locations found!")
        return None
    
    # Calculate center point
    avg_lat = sum(lat for lat, _, _ in locations) / len(locations)
    avg_lon = sum(lon for _, lon, _ in locations) / len(locations)
    
    # Create map centered on Manhattan
    m = folium.Map(
        location=[avg_lat, avg_lon],
        zoom_start=12,
        tiles='OpenStreetMap'
    )
    
    # Add Manhattan polygon boundary
    try:
        polygon = _get_manhattan_polygon()
        # Convert polygon coordinates to folium format (lat, lon)
        # Shapely polygon uses (lon, lat), folium needs (lat, lon)
        polygon_coords = [[lat, lon] for lon, lat in polygon.exterior.coords]
        folium.Polygon(
            locations=polygon_coords,
            color='blue',
            weight=3,
            fill=True,
            fillColor='blue',
            fillOpacity=0.1,
            popup='Manhattan Boundary',
            tooltip='Manhattan Polygon Boundary'
        ).add_to(m)
        print("✅ Added Manhattan polygon boundary to map")
    except Exception as e:
        print(f"⚠️  Could not add Manhattan polygon to map: {e}")
    
    # Add marker cluster for better performance with many markers
    marker_cluster = MarkerCluster().add_to(m)
    
    # Add markers with restaurant names
    for lat, lon, name in locations:
        folium.CircleMarker(
            location=[lat, lon],
            radius=5,
            popup=folium.Popup(
                f"<b>{name}</b>",
                max_width=300,
                parse_html=True
            ),
            tooltip=name,
            color='red',
            fill=True,
            fillColor='red',
            fillOpacity=0.7,
            weight=2
        ).add_to(marker_cluster)
    
    # Save HTML map
    m.save(output_html)
    print(f"✅ Created interactive map: {output_html}")
    return m

def render_map_to_image(html_file: str, output_image: str = "restaurants_map.png", width: int = 1920, height: int = 1080):
    """Render HTML map to static image using Playwright."""
    print(f"Rendering map to image: {output_image}...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': width, 'height': height})
        
        # Load the HTML file
        html_path = Path(html_file).absolute()
        page.goto(f'file://{html_path}')
        
        # Wait for map to load
        time.sleep(3)
        
        # Take screenshot
        page.screenshot(path=output_image, full_page=False)
        browser.close()
    
    print(f"✅ Created static map image: {output_image}")

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Visualize restaurant locations on a map')
    parser.add_argument(
        '--input',
        default='manhattan_restaurants.json',
        help='Input JSON file with restaurants'
    )
    parser.add_argument(
        '--output-html',
        default='restaurants_map.html',
        help='Output HTML map file'
    )
    parser.add_argument(
        '--output-image',
        default='restaurants_map.png',
        help='Output static image file'
    )
    parser.add_argument(
        '--html-only',
        action='store_true',
        help='Only create HTML map, skip image rendering'
    )
    parser.add_argument(
        '--width',
        type=int,
        default=1920,
        help='Image width in pixels (default: 1920)'
    )
    parser.add_argument(
        '--height',
        type=int,
        default=1080,
        help='Image height in pixels (default: 1080)'
    )
    
    args = parser.parse_args()
    
    # Load restaurants
    print(f"Loading restaurants from {args.input}...")
    restaurants = load_restaurants(args.input)
    print(f"Loaded {len(restaurants)} restaurants")
    
    # Extract locations
    locations = extract_locations(restaurants)
    print(f"Found {len(locations)} restaurants with valid locations")
    
    if not locations:
        print("No valid locations found!")
        sys.exit(1)
    
    # Create map
    print("Creating map...")
    create_map(locations, args.output_html)
    
    # Render to image if requested
    if not args.html_only:
        try:
            render_map_to_image(args.output_html, args.output_image, args.width, args.height)
        except Exception as e:
            print(f"⚠️  Could not render image: {e}")
            print(f"   HTML map saved at: {args.output_html}")
            print(f"   You can open it in a browser and take a screenshot manually")
    else:
        print(f"HTML-only mode: Open {args.output_html} in your browser to view the map")

if __name__ == "__main__":
    main()
