#!/usr/bin/env python3

import json
import os
import itertools
from dataclasses import dataclass

import geopandas as gpd
import shapely
import tqdm

import graphexplore
from pipeline_interface import PipelineInterface, PipelineResult
from constants import datasets_path, shapefile_path

"""
Add shell script to run pipeline

- add debugging info: raw route count, min route #
- output path id numbers
- Need to pick points in graph that are reachable
"""


@dataclass
class PointInfo:
    filename: str
    index_col: str


class Network:
    BUSINESS_POINTS = PointInfo(datasets_path() / 'chicago' / 'Business Licenses - Current Active - Map.geojson',
                                'license_id')

    def __init__(self, finder: graphexplore.NxFinder2, point_index: str):
        self.finder = finder
        self.point_index = point_index

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
        cross = cf.merge(cf[self.point_index], how='cross')
        inputs = []
        for _, row in cross.iterrows():
            rx = row[f'{self.point_index}_x']
            ry = row[f'{self.point_index}_y']
            if rx == ry:
                continue
            iters += 1
            if limit and iters > limit:
                break
            inputs.append((rx, ry))
        paths = self.finder.route_edges(self.point_index, inputs)
        for path, rt in paths:
            #print(f'debug path: {path} {rt}')
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


class NetworkStage(PipelineInterface):

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        business_points = self.get_dependency('business_preprocess').get()
        area = self.get_dependency('community_area_filter').get()
        sample = self.stage_info['parameters']['sample_size']
        nxfinder = graphexplore.NxFinder2(area, business_points, silent=False, sample=sample)
        network = Network(nxfinder, self.stage_info['parameters']['points_key'])
        applied = network.apply()
        filt = applied[applied.geometry.type == 'LineString']
        rv.obj = filt
        return rv


class BusinessPreprocess(PipelineInterface):
    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        business = self.get_dependency('business_fetch').get()
        area = self.get_dependency('community_area_filter').get()
        business.clip(shapely.geometry.box(*list(area.total_bounds)))
        rv.obj = business
        return rv


class ShapefileOutput(PipelineInterface):
    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        netfile = self.get_dependency('network_analyze').get()
        rv.obj = netfile
        netfile.to_file(shapefile_path() / 'computed_bike_network.shp')
        return rv
