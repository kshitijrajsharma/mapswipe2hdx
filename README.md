# MapSwipe to HDX

## Overview
MapSwipe to HDX connects crowdsourced data from the MapSwipe app with HDX, making this valuable geographic information more accessible to humanitarian organizations. It aggregates results from multiple MapSwipe projects and publishes them as downloadable datasets in various GIS formats.

## Features
- Combine multiple MapSwipe projects into a single dataset
- Export data in multiple formats (GeoJSON, GeoPackage, KML, Shapefile)
- Automatically publish datasets to the HDX platform
- Configure through YAML files or environment variables

## Configuration
Create a YAML configuration file:

```yaml
# MapSwipe to HDX Configuration
hdx_site: "demo"  # Use "demo" or "prod" for production
hdx_api_key: "YOUR-HDX-API-KEY"
hdx_owner_org: "YOUR-HDX-ORG-ID"
hdx_maintainer: "YOUR-HDX-MAINTAINER-ID"

# Dataset Information
dataset_name: "MapSwipe Results: Myanmar Buildings"
dataset_location: "Myanmar"
dataset_frequency: "As needed"
dataset_tags:
  - "geodata"
  - "buildings"
  - "mapswipe"

# Output Settings
file_formats:
  - "geojson"
  - "gpkg"
  - "kml"
  - "shp"

# MapSwipe Projects to Process
projects:
  - project_id: "https://mapswipe.org/en/projects/-OMTfDy03ThqukWPeBVp/"
    name: "Myanmar: Disaster Response 1"
  - project_id: "12345"
    name: "Myanmar: Building Detection"
```

