#!/usr/bin/env python3

import os

import pandas as pd

import requests
from gtfs_functions import Feed

from pipeline_interface import PipelineInterface, PipelineResult


class TransitMerge(PipelineInterface):
    METRA_EXTRA = ['UP-NW',
                   'UP-N',
                   'NCS', 'UP-W', 'BNSF', 'SWS']
    METRA = {'Electric': 'ME',
             'Electric, S. Shore': 'ME',
             'UP-N, UP-NW, UP-W': 'UP-N',
             'UP-N, UP-NW': 'UP-N',
             'Milw-N, Milw-W, NCS': 'MD-N',
             'Milw-N': 'MD-N',
             'Milw-W, NCS': 'MD-W',
             'Milw-W': 'MD-W',
             'Rock Is.': 'RI',
             'S. Shore': 'SSL',
             'Electric-Main Line': 'ME',
             'Electric-Blue Island': 'ME',
             'Rock Is.-Main': 'RI',
             'Rock Is.-Branch': 'RI-B',
             'Heritage': 'HC',
             'Electric-South Corridor': 'ME',
             'Heritage, SWS': 'HC',
             'BNSF, Heritage, SWS': 'BNSF'
             }
    # 'BNSF', 'MD-N', 'MD-W', 'ME', 'RI', 'UP-N', 'UP-NW', 'UP-W'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process_metra(self):
        feed = self.get_dependency('metra_feed_load').get()
        routes = self.get_dependency('metra_routes_fetch').get()
        rlf = feed.lines_freq
        routes['route_id'] = routes.apply(lambda x: self.METRA.get(x.LINES, x.LINES), axis=1)
        feed2 = rlf.drop(columns=['geometry'])
        #maxtrips = feed2[feed2.window == '10:00-20:00'].groupby('route_name').apply(lambda x: x.loc[x.ntrips.idxmax()].drop(['route_name']))
        #print('max trips')
        #print(maxtrips)
        routes2 = routes[['route_id', 'DESCRIPTIO', 'geometry']]
        m = routes2.merge(feed2, left_on='route_id', right_on='route_id')
        print('metra pre-merge')
        print(m)
        print(type(m))
        #m = m[m.window == '10:00-20:00'].groupby('route_name').apply(lambda x: x.loc[x.ntrips.idxmax()].drop(['route_name']))
        m = m[m.window == '10:00-20:00']
        m.crs = 3435
        print('metra merge')
        print(m)
        return m

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
        metra = self.process_metra()
        rv.obj = pd.concat([rv.obj, pace, metra])
        print('merged')
        print(rv.obj)
        #rv.obj.to_file(os.path.join(DESTINATION_DIR, 'bus_route_frequency.shp'))
        #metra_raw = self.get_dependency('metra_gtfs_clean')
        #print(metra_raw)
        # metra_feed = Feed(metra_raw.get_filename(),
        #                  time_windows=[0, 10, 20, 24],
        #                  start_date='2023-09-17', end_date='2023-09-17')
        #metra_feed = self.get_dependency('metra_feed_load').get()
        #print(metra_feed.lines_freq)
        return rv
