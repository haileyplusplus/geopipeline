#!/usr/bin/env python3

import os

import geopandas as gpd
import shapely
import tqdm

import pathwrapper

# todo: merge all shapefile writing / processing
DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'

class Network:
    def __init__(self, finder: pathwrapper.Finder):
        self.finder = finder

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
        iters = 0
        cf = self.filter_points()
        cross = cf.merge(cf['school_nm'], how='cross')
        for _, row in tqdm.tqdm(cross.iterrows()):
            if row.school_nm_x == row.school_nm_y:
                continue
            iters += 1
            if limit and iters > limit:
                break
            path = self.finder.route_edges('school_nm', row.school_nm_x, row.school_nm_y)
            for p in path:
                segcounts[p] = segcounts.get(p, 0) + 1
        return segcounts, iters

    def apply(self):
        segcounts, iters = self.calculate_n2_network()
        new_df = self.finder.gdf.copy()
        def apply_fn(x):
            count = segcounts.get(x.trans_id, 0)
            return count * 1.0 / iters
        new_df['routegradient'] = new_df.apply(apply_fn, axis=1)
        df2 = new_df.drop(columns=[x for x in new_df.columns if new_df[x].dtype.name.startswith('datetime')])
        return df2


if __name__ == "__main__":
    f = pathwrapper.Finder('/Users/hailey/tmp/mapcache/BikeStreets.LINCOLN PARK.LAKE VIEW.geojson', '/tmp/schools.geojson')
    n = Network(f)
    applied = n.apply()
    filt = applied[applied.geometry.type == 'LineString']
    #print(applied)
    #print(filt)
    filt.to_file(os.path.join(DESTINATION_DIR, 'computed_bike_network.shp'))
