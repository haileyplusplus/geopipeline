#!/usr/bin/env python3

import os

import pandas as pd

import requests
from gtfs_functions import Feed

from pipeline_interface import PipelineInterface, PipelineResult

DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'


class TransitMerge(PipelineInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process_pace(self):
        feed = self.get_dependency('pace_feed_load').get()
        routes = self.get_dependency('pace_routes_fetch').get()
        rlf = feed.lines_freq
        rlf['raw_route'] = rlf.apply(lambda x: x.route_id.split('-')[0], axis=1)
        feed2 = rlf.drop(columns=['geometry'])
        routes2 = routes[['ROUTE', 'NAME', 'geometry']]
        m = routes2.merge(feed2, left_on='ROUTE', right_on='raw_route')
        print(m)
        print(type(m))
        m = m[m.window == '10:00-16:00'].groupby('route_name').apply(lambda x: x.loc[x.ntrips.idxmax()].drop(['route_name']))
        m.crs = 4326
        print(m)
        return m.to_crs(3435)

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        feed = self.get_dependency('feed_load').get()
        routes = self.get_dependency('routes_fetch').get()
        feed2 = feed.lines_freq.drop(columns=['geometry'])
        m = routes.merge(feed2, left_on='ROUTE', right_on='route_id')
        rv.obj = m[m.window == '10:00-16:00'].groupby('route_name').apply(lambda x: x.loc[x.ntrips.idxmax()].drop(['route_name']))
        rv.obj.crs = 3435
        pace = self.process_pace()
        rv.obj = pd.concat([rv.obj, pace])
        rv.obj.to_file(os.path.join(DESTINATION_DIR, 'bus_route_frequency.shp'))
        return rv
