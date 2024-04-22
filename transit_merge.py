#!/usr/bin/env python3

import os

import requests
from gtfs_functions import Feed

from pipeline_interface import PipelineInterface, PipelineResult

DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'


class TransitMerge(PipelineInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        feed = self.get_dependency('feed_load').get()
        routes = self.get_dependency('routes_fetch').get()
        feed2 = feed.lines_freq.drop(columns=['geometry'])
        m = routes.merge(feed2, left_on='ROUTE', right_on='route_id')
        rv.obj = m[m.window == '10:00-16:00'].groupby('route_name').apply(lambda x: x.loc[x.ntrips.idxmax()].drop(['route_name']))
        rv.obj.crs = 3435
        rv.obj.to_file(os.path.join(DESTINATION_DIR, 'bus_route_frequency.shp'))
        return rv
