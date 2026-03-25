#!/usr/bin/env python3
"""Add basemap layer switcher (Dark/Light/OSM) to EarthGrid HTML files."""

import sys

# --- beacon.html ---
with open('assets/beacon.html', 'r') as f:
    html = f.read()

old = "  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {\n    maxZoom: 18,\n    subdomains: 'abcd',\n  }).addTo(covMap);"

new = """  const darkLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 18, subdomains: 'abcd',
  });
  const lightLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 18, subdomains: 'abcd',
  });
  const osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
  });
  darkLayer.addTo(covMap);
  L.control.layers({'Dark': darkLayer, 'Light': lightLayer, 'OSM': osmLayer}, null, {position: 'topright'}).addTo(covMap);"""

if old not in html:
    print("ERROR: beacon.html target not found", file=sys.stderr)
    sys.exit(1)
html = html.replace(old, new)
with open('assets/beacon.html', 'w') as f:
    f.write(html)
print("beacon.html patched")

# --- ui.html ---
with open('assets/ui.html', 'r') as f:
    html = f.read()

old2 = "  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {\n    attribution: '\u00a9 OSM \u00a9 CARTO', maxZoom: 19\n  }).addTo(_fetchMap);"

new2 = """  const darkTile = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '\u00a9 OSM \u00a9 CARTO', maxZoom: 19
  });
  const lightTile = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
    attribution: '\u00a9 OSM \u00a9 CARTO', maxZoom: 19
  });
  const osmTile = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '\u00a9 OSM', maxZoom: 19
  });
  darkTile.addTo(_fetchMap);
  L.control.layers({'Dark': darkTile, 'Light': lightTile, 'OSM': osmTile}, null, {position: 'topright'}).addTo(_fetchMap);"""

if old2 not in html:
    print("ERROR: ui.html target not found", file=sys.stderr)
    sys.exit(1)
html = html.replace(old2, new2)
with open('assets/ui.html', 'w') as f:
    f.write(html)
print("ui.html patched")
