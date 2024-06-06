#!/usr/bin/env python3

from pipeline_interface import PipelineInterface, PipelineResult
from constants import shapefile_path


class OsmShapefile(PipelineInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        rv.obj = self.get_dependency("osm_roads_preprocess").get()
        rv.obj.to_file(shapefile_path() / 'osm_roads.shp')
        return rv
