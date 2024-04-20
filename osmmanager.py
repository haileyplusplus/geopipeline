#!/usr/bin/env python3

import zipfile
import geopandas as gpd
import os

from pipeline_interface import PipelineInterface, PipelineResult


DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'


class OsmShapefile(PipelineInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        rv.obj = self.get_dependency("osm_roads_preprocess").get()
        rv.obj.to_file(os.path.join(DESTINATION_DIR, 'osm_roads.shp'))
        return rv
