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
    def __init__(self, filename, points_filename, silent=False):
        self.filename = filename
        self.points_filename = points_filename
        self.gdf = gpd.read_file(self.filename)
        self.points_df = gpd.read_file(self.points_filename)
        self.silent = silent

    def reset_points(self, points_filename):
        self.points_filename = points_filename
        self.points_df = gpd.read_file(self.points_filename)

    def closest_point(self, pointrow):
        # this is slow (iterates over entire dataframe)
        p = pointrow.iloc[0].geometry
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
        return minr

    def make_gdf(self, ids):
        rv = []
        for id_ in ids:
            row = self.gdf[self.gdf['trans_id'] == id_].iloc[0]
            rv.append(row)
        return gpd.GeoDataFrame(rv)

    def router(self, colname, tups: List[Tuple[str, str]]):
        points = []
        for start, end in tqdm.tqdm(tups):
            startpoint = self.closest_point(self.points_df[self.points_df[colname] == start])
            endpoint = self.closest_point(self.points_df[self.points_df[colname] == end])
            if startpoint.empty or endpoint.empty:
                continue
            pointmap = lambda x: {'type': 'Feature', 'properties': {}, 'geometry': shapely.geometry.mapping(x.boundary.geoms[0])}
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
    f = Finder('/Users/hailey/tmp/mapcache/BikeStreets.LINCOLN PARK.LAKE VIEW.geojson', '/tmp/schools.geojson')
    #rj = f.router('school_nm', 'LAKE VIEW HS', 'LASALLE')
    #rj = f.router('school_nm', 'LASALLE', 'LAKE VIEW HS')

    for r in f.route_edges('school_nm', [('LASALLE', 'LAKE VIEW HS'), ('PRESCOTT', 'NEWBERRY')]):
        print(f'Routing {r}')
        route = f.make_gdf(r)
        print(route)

