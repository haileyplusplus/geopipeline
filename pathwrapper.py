#!/usr/bin/env python3

import json
import os
import subprocess
import sys
from typing import List, Tuple

import geopandas
import geopandas as gpd
import shapely
import tqdm


class Finder:
    CHICAGO_CRS = 26916

    def __init__(self, filename, points_filename, silent=False, sample=None):
        self.filename = filename
        self.points_filename = points_filename
        self.gdf = gpd.read_file(self.filename)
        self.points_df = gpd.read_file(self.points_filename)
        self.points_alt = self.points_df.to_crs(self.CHICAGO_CRS)
        self.gdf_alt = self.gdf.to_crs(self.CHICAGO_CRS)
        if sample and sample < len(self.points_df):
            self.points_df = self.points_df.sample(sample)
        self.silent = silent
        self.pointrow_cache = {}

    def reset_points(self, points_filename):
        self.points_filename = points_filename
        self.points_df = gpd.read_file(self.points_filename)

    def closest_point(self, pointrow):
        # this is slow (iterates over entire dataframe)
        p = pointrow.iloc[0].geometry
        cached = self.pointrow_cache.get(p)
        if cached is not None:
            return cached
        #print(type(pointrow))
        #print(type(p))
        assert type(p) is shapely.geometry.point.Point
        mind = -1
        minr = None
        for i, x in self.gdf.iterrows():
            dist = x.geometry.distance(pointrow.geometry).iloc[0]
            if minr is None or dist < mind:
                mind = dist
                minr = x
        self.pointrow_cache[p] = minr
        return minr

    def closest_point2(self, pointrow):
        p = pointrow.iloc[0].geometry
        cached = self.pointrow_cache.get(p)
        if cached is not None:
            return cached
        alt = self.gdf_alt.clip(pointrow.geometry.buffer(20))
        if alt.empty:
            self.pointrow_cache[p] = geopandas.GeoDataFrame()
            return geopandas.GeoDataFrame()
        alt = alt.sample(1)
        rv = self.gdf[self.gdf.trans_id == alt.trans_id.iloc[0]].iloc[0]
        self.pointrow_cache[p] = rv
        return rv

    def make_gdf(self, ids):
        rv = []
        for id_ in ids:
            row = self.gdf[self.gdf['trans_id'] == id_].iloc[0]
            rv.append(row)
        return gpd.GeoDataFrame(rv)

    def router(self, colname, tups: List[Tuple[str, str]]):
        points = []
        for start, end in tqdm.tqdm(tups):
            #startpoint = self.closest_point(self.points_df[self.points_df[colname] == start])
            #endpoint = self.closest_point(self.points_df[self.points_df[colname] == end])
            startpoint = self.closest_point2(self.points_alt[self.points_alt[colname] == start])
            endpoint = self.closest_point2(self.points_alt[self.points_alt[colname] == end])
            if startpoint.empty or endpoint.empty:
                continue
            pointmap = lambda x: {'type': 'Feature', 'properties': {}, 'geometry': shapely.geometry.mapping(x.boundary.geoms[0])}
            #print(startpoint)
            #print(startpoint.geometry)
            sp = pointmap(startpoint.geometry)
            ep = pointmap(endpoint.geometry)
        #sp = json.loads(startpoint.to_json())['features'][0]
        #ep = json.loads(endpoint.to_json())['features'][0]
            points.append({'start': sp, 'end': ep, 'startname': start, 'endname': end})
        with open('/tmp/pointsfile.json', 'w') as fh:
            json.dump(points, fh)
        os.chdir('/Users/hailey/projcode/routing')
        args = ['/Users/hailey/.nvm/versions/node/v21.6.2/bin/npx',
                'tsx',
                'route.ts',
                self.filename,
                '/tmp/pointsfile.json',
                'trans_id',
                ]
        output = sys.stdout
        if self.silent:
            output = subprocess.DEVNULL
        cp = subprocess.run(args, stdout=output)
        if cp.returncode == 0:
            #print(os.stat('/tmp/path.json'))
            return json.load(open('/tmp/path.json'))
        return None

    def route_edges(self, colname, tups: List[Tuple[str, str]]):
        rj2 = self.router(colname, tups)
        for x in rj2:
            yield list(x['edgeDatas'])


if __name__ == "__main__":
    #f = Finder(sys.argv[1], sys.argv[2])
    schools_filename = '/Users/hailey/datasets/chicago/Chicago Public Schools - School Locations SY1819.geojson'
    f = Finder('/Users/hailey/tmp/mapcache/BikeStreets.LINCOLN PARK.LAKE VIEW.geojson', schools_filename)
    #rj = f.router('school_nm', 'LAKE VIEW HS', 'LASALLE')
    #rj = f.router('school_nm', 'LASALLE', 'LAKE VIEW HS')

    for r in f.route_edges('school_nm', [('LASALLE', 'LAKE VIEW HS'), ('PRESCOTT', 'NEWBERRY')]):
        print(f'Routing {r}')
        route = f.make_gdf(r)
        print(route)

