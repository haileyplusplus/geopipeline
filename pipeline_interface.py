from abc import ABC, abstractmethod
from dataclasses import dataclass

import geopandas as gpd
import os
import uuid
import datetime

@dataclass
class PipelineResult:
    obj = None
    filename: str = None
    updated: datetime.datetime = None

    def __init__(self, obj=None, filename=None):
        self.obj = obj
        self.filename = filename
        self.updated = datetime.datetime.now()

    def __str__(self):
        if self.obj is not None:
            return f'obj {self.obj}'
        elif self.filename:
            return f'fn {self.filename}'
        else:
            return 'Empty PipelineResult'

    def empty(self):
        return self.obj is None and self.filename is None

    def get(self):
        if self.empty():
            raise ValueError
        if self.obj is None:
            # handle other types
            return gpd.read_file(self.filename)
        return self.obj

    def serialize(self, dir_):
        assert self.filename is None
        assert not self.empty()
        filename = str(uuid.uuid1())
        filepath = os.path.join(dir_, filename)
        self.obj.to_file(filepath, driver='GeoJSON')
        return filename


class PipelineInterface(ABC):
    def __init__(self, stage_info: dict):
        """

        :type stage_info: dict
        """
        self.stage_info: dict = stage_info
        self.depend_results = {}

    @abstractmethod
    def run_stage(self) -> PipelineResult:
        pass

    def set_results(self, results):
        self.depend_results = results

    def get_dependency(self, name):
        # need to assert that dependency is actually in config
        return self.depend_results[name]