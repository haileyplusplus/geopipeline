#!/usr/bin/env python3

import os
import sys
from typing import List

import geopandas as gpd

COMMUNITY_AREAS = '/Users/hailey/tmp/mapcache/Boundaries - Community Areas (current).geojson'


def filter(gdf: gpd.GeoDataFrame, community_areas: List[str]) -> gpd.GeoDataFrame:
    neighborhoods = gpd.read_file(COMMUNITY_AREAS)
    # lower case
    n = neighborhoods[neighborhoods.community.isin(community_areas)]
    if n.empty:
        print(f'Neighborhood (community area) {community_areas} not found')
        return gpd.GeoDataFrame()
    return gdf.clip(n.envelope)


if __name__ == "__main__":
    filename = sys.argv[1]
    neighborhoods = sys.argv[2:]
    path, ext = os.path.splitext(filename)
    gdf = gpd.read_file(filename)
    filtered = filter(gdf, neighborhoods)
    if filtered.empty:
        sys.exit(1)
    print(filtered)
    nstr = '.'.join(neighborhoods)
    output = f'{path}.{nstr}{ext}'
    print(f'Filtered to {output}')
    filtered.to_file(output, driver='GeoJSON')

