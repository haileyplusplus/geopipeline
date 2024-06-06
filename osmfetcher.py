#!/usr/bin/env python3

import zipfile
import geopandas as gpd

from pipeline_interface import PipelineInterface, PipelineResult
from constants import datasets_path


OSM_SOURCE = datasets_path() / 'osm' / 'illinois-latest-free.shp.zip'

# gis_osm_roads_free_1


class OsmExtractor(PipelineInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = zipfile.ZipFile(OSM_SOURCE)
        self.shapefiles = [x for x in self.source.infolist() if x.filename.endswith('.shp')]

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        filename = self.stage_info['parameters']['filename'] + '.shp'
        print(f'check for {filename}')
        assert filename in set([x.filename for x in self.shapefiles])
        zipfile_url = f'zip://{OSM_SOURCE}!{filename}'
        rv.obj = gpd.read_file(zipfile_url)
        return rv


class OsmPreprocess(PipelineInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        roads = self.get_dependency("osm_roads_fetch").get()
        city_boundary = self.get_dependency("city_boundary_fetch").get()
        rv.obj = roads.clip(city_boundary.envelope)
        return rv
