#!/usr/bin/env python3

import os
import sys
from typing import List

import geopandas
import geopandas as gpd
import shapely

from pipeline_interface import PipelineInterface, PipelineResult


class BoundaryFilter(PipelineInterface):
    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        gdf = self.get_dependency('bikestreets_off_join').get()
        boundaries: geopandas.GeoDataFrame = self.get_dependency('community_areas_fetch').get()
        # lower case
        params: dict = self.stage_info['parameters']
        boundary_field = params['field']
        values = params['values']
        n = boundaries[boundaries[boundary_field].isin(values)]
        if n.empty:
            print(f'No boundary in {values} found')
            rv.obj = gpd.GeoDataFrame()
        else:
            rv.obj = gdf.clip(n.envelope)
        return rv
