#!/usr/bin/env python3

import os
import sys

import geopandas as gpd

COMMUNITY_AREAS = '/Users/hailey/tmp/mapcache/Boundaries - Neighborhoods.geojson'


def filter(gdf: gpd.GeoDataFrame, community_area: str) -> gpd.GeoDataFrame:
    neighborhoods = gpd.read_file(COMMUNITY_AREAS)
    # lower case
    n = neighborhoods[neighborhoods.pri_neigh == community_area]
    if n.empty:
        print(f'Neighborhood (community area) {community_area} not found')
        return gpd.GeoDataFrame()
    return gdf.clip(n.envelope)


if __name__ == "__main__":
    filename = sys.argv[1]
    neighborhood = sys.argv[2]
    path, ext = os.path.splitext(filename)
    gdf = gpd.read_file(filename)
    filtered = filter(gdf, neighborhood)
    if filtered.empty:
        sys.exit(1)
    print(filtered)
    output = f'{path}.{neighborhood}{ext}'
    print(f'Filtered to {output}')
    filtered.to_file(output, driver='GeoJSON')

