#!/usr/bin/env python3

import json
import os
import itertools
from dataclasses import dataclass

import geopandas as gpd
import shapely
import tqdm

import pathwrapper

# todo: merge all shapefile writing / processing
DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'

"""
Add shell script to run pipeline

- add debugging info: raw route count, min route #
- output path id numbers
"""

@dataclass
class PointInfo:
    filename: str
    index_col: str

class Network:
    SCHOOL_POINTS = PointInfo('/tmp/schools.geojson', 'school_nm')
    BUSINESS_POINTS = PointInfo('/Users/hailey/datasets/chicago/Business Licenses - Current Active - Map.geojson', 'license_id')

    def __init__(self, finder: pathwrapper.Finder, point_info: PointInfo):
        self.finder = finder
        self.point_info = point_info

    def filter_points(self):
        # approx heuristic
        f = self.finder
        return f.points_df.clip(shapely.geometry.box(*list(f.gdf.total_bounds)))

    def calculate_n2_network(self, limit=None):
        """
        Run through all point pairs (bidirectionally) and store segment counts
        :return: Segment counts
        """
        segcounts = {}
        routes = {}
        iters = 0
        cf = self.filter_points()
        cross = cf.merge(cf[self.point_info.index_col], how='cross')
        inputs = []
        for _, row in cross.iterrows():
            rx = row[f'{self.point_info.index_col}_x']
            ry = row[f'{self.point_info.index_col}_y']
            if rx == ry:
                continue
            iters += 1
            if limit and iters > limit:
                break
            inputs.append((rx, ry))
        paths = self.finder.route_edges(self.point_info.index_col, inputs)
        for path, rt in paths:
            for p in path:
                segcounts[p] = segcounts.get(p, 0) + 1
                routes[p] = max(routes.get(p, -1), rt)
        return segcounts, iters, routes

    def apply(self):
        segcounts, iters, routes = self.calculate_n2_network()
        with open('/tmp/raw_segcounts.json', 'w') as fh:
            json.dump({'segcounts': segcounts, 'iters': iters}, fh)
        new_df = self.finder.gdf.copy()
        def apply_fn(x):
            count = segcounts.get(x.trans_id, 0)
            return count * 1.0 / iters
        new_df['routegradient'] = new_df.apply(apply_fn, axis=1)
        new_df['routesamp'] = new_df.apply(lambda x: routes.get(x.trans_id, -1), axis=1)
        new_df['rtraw'] = new_df.apply(lambda x: segcounts.get(x.trans_id, -1), axis=1)
        df2 = new_df.drop(columns=[x for x in new_df.columns if new_df[x].dtype.name.startswith('datetime')])
        return df2


if __name__ == "__main__":
    # need to hook this up
    #SAMPLE_SIZE = 2000
    SAMPLE_SIZE = 100
    filtered_file = open('/tmp/filterfile.txt').read().strip()
    print(f'Reading from {filtered_file}')
    point_info = Network.BUSINESS_POINTS
    #point_info = Network.SCHOOL_POINTS
    points_filename = point_info.filename
    f = pathwrapper.Finder(filtered_file, points_filename, silent=False, sample=SAMPLE_SIZE)
    n = Network(f, point_info)
    applied = n.apply()
    filt = applied[applied.geometry.type == 'LineString']
    #print(applied)
    #print(filt)
    filt.to_file('/tmp/computed_bike_network.geojson', driver='GeoJSON')
    filt.to_file(os.path.join(DESTINATION_DIR, 'computed_bike_network.shp'))
