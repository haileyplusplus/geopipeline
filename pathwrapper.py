#!/usr/bin/env python3

import json
import os
import subprocess
import sys

import geopandas
import geopandas as gpd
import shapely


class Finder:
    def __init__(self, filename, points_filename):
        self.filename = filename
        self.points_filename = points_filename
        self.gdf = gpd.read_file(self.filename)
        self.points_df = gpd.read_file(self.points_filename)

    def closest_point(self, pointrow):
        p = pointrow.iloc[0].geometry
        print(type(pointrow))
        print(type(p))
        assert type(p) is shapely.geometry.point.Point
        mind = -1
        minr = None
        for i, x in self.gdf.iterrows():
            dist = x.geometry.distance(pointrow.geometry).iloc[0]
            if minr is None or dist < mind:
                mind = dist
                minr = x
        return minr

    def router(self, colname, start, end):
        startpoint = self.closest_point(self.points_df[self.points_df[colname] == start])
        endpoint = self.closest_point(self.points_df[self.points_df[colname] == end])
        if startpoint.empty or endpoint.empty:
            return False
        print(startpoint)
        pointmap = lambda x: {'type': 'Feature', 'properties': {}, 'geometry': shapely.geometry.mapping(x.boundary.geoms[0])}
        sp = pointmap(startpoint.geometry)
        ep = pointmap(endpoint.geometry)
        #sp = json.loads(startpoint.to_json())['features'][0]
        #ep = json.loads(endpoint.to_json())['features'][0]
        points = {'start': sp, 'end': ep}
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
        cp = subprocess.run(args)
        if cp.returncode == 0:
            print(os.stat('/tmp/path.geojson'))
            return True
        return False


if __name__ == "__main__":
    #f = Finder(sys.argv[1], sys.argv[2])
    f = Finder('/tmp/bikelp.json', '/tmp/schools.geojson')
    f.router('school_nm', 'LINCOLN PARK HS', 'LASALLE')
