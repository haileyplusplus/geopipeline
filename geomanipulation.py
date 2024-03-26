#!/usr/bin/env python3

import os
import sys

import geopandas
import pandas as pd

"""
Cheap Jupyter notebook


gdf = geopandas.from_json('3w5d-sru8.geojson')
gdf = geopandas.load_file('3w5d-sru8.geojson')
gdf = geopandas.read_file('3w5d-sru8.geojson')
gdf.crs
gdf.crs['units']
gdf
gdf.crs
gdf.to_crs(epsg=4326)
gdf.to_crs(epsg=4326).crs
gdf.crfs
gdf.cs
gdf.crs
gdf.crs.axis_info
gdf.crs.axis_info.uints
gdf.crs.axis_info.unit_name
gdf.crs.axis_info
gdf.to_crs(epsg=2263).crs
gdf.to_crs(epsg=26916).crs
gdf.to_crs(epsg=26916)
gdf.crs
gdf2 = gdf.to_crs(epsg=26916)
gdf2
gdf2.buffer(10)
gdf2.buffer(10).to_crs(epsg=4326)
gdf
gdf2.buffer(10).to_crs(epsg=4326)
gdf2.buffer(10).to_crs(epsg=4326).plot()
import matplotlib
dir(matplotlib)
matplotlib.pyplot.show()
gdf2.buffer(20).to_crs(epsg=4326).plot()
matplotlib.pyplot.show()
history
import readline; 
"""


class Wrapper:
    DATA_DIR = '/Users/hailey/datasets/chicago'

    def __init__(self, filename):
        self.orig = geopandas.read_file(os.path.join(self.DATA_DIR, filename))
        # for the Chicago area, unit in meters
        self.layer = self.orig.to_crs(epsg=26916)

    def get_buffer(self):
        return self.layer.buffer(2)

    def get_street(self, name):
        colname = 'st_name'
        if colname not in self.layer.columns:
            colname = 'street_nam'
        return self.layer[self.layer[colname] == name]


if __name__ == "__main__":
    streets = Wrapper('Street Center Lines.geojson')
    bike_routes = Wrapper('Bike Routes.geojson')
    halsted1 = streets.get_street('HALSTED')
    print(bike_routes.layer)
    halsted2 = bike_routes.get_street('HALSTED')
    matched = None
    cumulative = geopandas.GeoDataFrame()
    for j, route in halsted2.iterrows():
        #print(route)
        r = route
        matching = []
        for i, seg in halsted1.iterrows():
            if route.geometry.buffer(2).contains(seg.geometry):
                matching.append(seg)
                s = seg
        if not matching:
            continue
        df = geopandas.GeoDataFrame(matching)
        rdf = pd.DataFrame([route.drop('geometry')])
        m = df.merge(rdf)
        cumulative = pd.concat([cumulative, m])
        #matched = df
    print(cumulative)
