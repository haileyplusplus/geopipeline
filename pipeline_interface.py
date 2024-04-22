from abc import ABC, abstractmethod
from dataclasses import dataclass

import geopandas as gpd
import os
import uuid
import datetime
import pickle

@dataclass
class PipelineResult:
    obj = None
    filename: str = None
    updated: datetime.datetime = None
    objtype: str = None
    error: str = None

    def __init__(self, obj=None, filename=None, objtype=None, error=None):
        self.obj = obj
        self.filename = filename
        self.updated = datetime.datetime.now()
        self.objtype = objtype
        self.error = error


    @classmethod
    def from_cached(cls, filename, objtype):
        return cls(obj=None, filename=filename, objtype=objtype)

    @classmethod
    def mark_incomplete(cls):
        """
        Indicate that this stage isn't implemented or cacheable.
        :return:
        """
        return cls(obj='incomplete', objtype='incomplete')

    @classmethod
    def as_error(cls, error):
        return cls(error=error)

    def valid(self):
        """

        :return: Whether object is in a valid state.
        """
        if self.error:
            return True
        if self.empty():
            return True
        if self.filename:
            return self.objtype is not None
        return True

    def __str__(self):
        if self.obj is not None:
            return f'obj of type {self.objtype}'
        elif self.filename:
            return f'fn {self.filename}'
        elif self.error:
            return f'PipelineResult has error: {self.error}'
        else:
            return 'Empty PipelineResult'

    def empty(self):
        return self.obj is None and self.filename is None

    def has_error(self):
        return self.error is not None

    def get(self):
        if self.empty():
            raise ValueError
        if self.obj is None:
            assert self.valid()
            if self.objtype == 'geopandas.GeoDataFrame':
                return gpd.read_file(self.filename)
            elif self.objtype == '$picklefile':
                with open(self.filename, 'rb') as fh:
                    self.obj = pickle.load(fh)
                    return self.obj
            raise ValueError(f'Object type {self.objtype} not handled.')
        return self.obj

    def serialize(self, dir_, objtype):
        assert self.filename is None
        assert not self.empty()
        assert not self.has_error()
        if self.objtype == 'incomplete':
            print(f'Skipping serialization of incomplete stage')
            return None
        self.objtype = objtype
        print(f'Serializing object of {self.objtype}')
        filename = str(uuid.uuid1())
        filepath = os.path.join(dir_, filename)
        if self.objtype == 'geopandas.GeoDataFrame':
            self.obj.to_file(filepath, driver='GeoJSON')
        elif self.objtype == '$bytesfile':
            with open(filepath, 'wb') as fh:
                fh.write(self.obj)
        elif self.objtype == '$picklefile':
            with open(filepath, 'wb') as fh:
                pickle.dump(self.obj, fh)
        else:
            raise ValueError(f'Object type {self.objtype} not handled.')
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