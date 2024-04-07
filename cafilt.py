#!/usr/bin/env python3

import os
import sys
from typing import List

import geopandas as gpd

COMMUNITY_AREAS = '/Users/hailey/tmp/mapcache/Boundaries - Community Areas (current).geojson'
MAP_CACHE = '/Users/hailey/tmp/mapcache'


def filter(gdf: gpd.GeoDataFrame, community_areas: List[str]) -> gpd.GeoDataFrame:
    neighborhoods = gpd.read_file(COMMUNITY_AREAS)
    # lower case
    n = neighborhoods[neighborhoods.community.isin(community_areas)]
    if n.empty:
        print(f'Neighborhood (community area) {community_areas} not found')
        return gpd.GeoDataFrame()
    return gdf.clip(n.envelope)

def filter_file(filename: str, neighborhoods: List[str]):
    """
    Must be a file relative to mapcache
    :param filename:
    :param neighborhoods:
    :return:
    """
    filepath = os.path.join(MAP_CACHE, filename)
    path, ext = os.path.splitext(filepath)
    gdf = gpd.read_file(filepath)
    filtered = filter(gdf, neighborhoods)
    if filtered.empty:
        sys.exit(1)
    nstr = '.'.join(neighborhoods)
    output = f'{path}.{nstr}{ext}'
    print(f'Filtered to {output}')
    filtered.to_file(output, driver='GeoJSON')
    with open('/tmp/filterfile.txt', 'w') as ff:
        ff.write(output)
        ff.write('\n')


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

