#!/usr/bin/env python3

import os
import sys

import geopandas
import pandas as pd
pd.set_option("mode.copy_on_write", True)


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
    DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'

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

    def get_streets(self):
        colname = 'st_name'
        if colname not in self.layer.columns:
            colname = 'street_nam'
        return set(self.layer[colname].unique())


class BikeMerge:
    """
    Fixes needed:
     - King Dr
     - Keeler
     - Elston
     - Plymouth

     Add processing to clean up messy booleans

     brl['contraflow'] = brl.contraflow.apply(repy)
     derive
       brl['street_bike_oneway'] = brl.apply(lambda x: f'{x.oneway_dir}-{x.br_ow_dir}', axis=1)
    br.layer['contraflow'] = br.layer.contraflow.apply(lambda x: {True: 'Y', False: 'N'}[x == 'Y'])
    """
    def __init__(self):
        self.streets = Wrapper('Street Center Lines.geojson')
        self.bike_routes = Wrapper('Bike Routes.geojson')
        self.bike_routes.layer.contraflow = self.bike_routes.layer.contraflow.replace(to_replace={None: 'N', '-': 'N'})
        self.output = geopandas.GeoDataFrame()
        self.bike_streets = self.bike_routes.get_streets() & self.streets.get_streets()

    def merge_street(self, street_name):
        print(f'Processing {street_name}')
        street = self.streets.get_street(street_name)
        bike_route = self.bike_routes.get_street(street_name)
        cumulative = geopandas.GeoDataFrame()
        for ri, route in bike_route.iterrows():
            matching = []
            rgbuffer = route.geometry.buffer(3)
            for si, seg in street.iterrows():
                match = rgbuffer.contains(seg.geometry)
                if match:
                    matching.append(seg.drop(['create_tim', 'update_tim', 'status_dat']))
            matched_frame = geopandas.GeoDataFrame(matching)
            route_frame = pd.DataFrame([route.drop('geometry')])
            if matched_frame.empty or route_frame.empty:
                print(f'  Empty frame: {matched_frame}, {route_frame}')
                continue
            m = matched_frame.merge(route_frame, left_on='street_nam', right_on='st_name')
            cumulative = pd.concat([cumulative, m])
        if not cumulative.empty:
            self.output = pd.concat([self.output, cumulative])

    def merge_all(self):
        for streetname in self.bike_streets:
            self.merge_street(streetname)

    def normalize(self):
        if self.output.empty:
            return None
        self.output.crs = 26916
        return self.output.to_crs(epsg=4326)

    def output_file(self, filename):
        self.normalize().to_file(filename, driver='GeoJSON')


if __name__ == "__main__":
    bm = BikeMerge()
    #bm.merge_street('HALSTED')
    #bm.merge_street('MARQUETTE')
    #bm.merge_street('CLYBOURN')
    bm.merge_all()
    df = bm.normalize()
    print(df)
    #print(df.iloc[0])
    #print(df)
    bm.output_file('bikes.geojson')
    df.to_file(os.path.join(Wrapper.DESTINATION_DIR, 'bikes.shp'))
