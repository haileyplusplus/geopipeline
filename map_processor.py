#!/usr/bin/env python3

import os
import glob
import sys

from abc import abstractmethod, ABC

import geopandas

"""
 Todo:
  - fix output name to not be .geojson.shp
"""

DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'
MAP_CACHE = '/Users/hailey/tmp/mapcache'
#db = SqliteDatabase('/Users/hailey/datasets/processed-data.sqlite3')
SOURCE_DIRS = ['/Users/hailey/datasets/cook',
               '/Users/hailey/datasets/chicago',
               '/Users/hailey/datasets/chicago/manual']

class ProcessorInterface(ABC):
    @abstractmethod
    def process(self, df):
        pass

    @staticmethod
    def fix_bool(x):
        if x and x.upper() == 'Y':
            return 'Y'
        return 'N'


class BikeRouteProcessor(ProcessorInterface):
    FILENAME = 'Bike Routes.geojson'

    def __init__(self):
        super().__init__()

    def matches(self, filename):
        assert filename.find('/') == -1
        return filename == self.FILENAME

    @staticmethod
    def bike_ow(x):
        if x.contraflow == 'N' and x.br_oneway == 'Y':
            return 'Y'
        return 'N'

    @staticmethod
    def fix_street_name(street_name):
        """
        Converts bike lane street names to normalized Street Center Lines names
        """
        return {'AVENUE': 'AVENUE L',
                'PLLYMOUTH': 'PLYMOUTH',
                'MARTIN LUTHER KING JR': 'DR MARTIN LUTHER KING JR',
                }.get(street_name, street_name).upper()

    def process(self, df):
        for c in ['contraflow', 'br_oneway']:
            df[c] = df[c].apply(self.fix_bool)
        df['bike_ow'] = df.apply(self.bike_ow, axis=1)
        df.st_name = df.st_name.apply(self.fix_street_name)
        return df


class StreetProcessor(ProcessorInterface):
    """
    ewns_coord, ewns_dir: SEDGWICK is 400W
    dir_travel: use for one-way. F, T, B
    status_code: C, N, P, UC, UR, V

    text histogram: value counts
    """
    FILENAME = 'Street Center Lines.geojson'
    CLASS_MAP = {'1': 'expr',
                 '2': 'art',
                 '3': 'coll',
                 '4': 'neigh',
                 '5': 'alley',
                 '7': 'tier',
                 '9': 'ramp',
                 'E': 'extent',
                 'RIV': 'river',
                 'S': 'sdwk',
                 '99': 'unk'}

    def __init__(self):
        super().__init__()

    def matches(self, filename):
        assert filename.find('/') == -1
        return filename == self.FILENAME

    @staticmethod
    def fix_street_name(street_name):
        """
        Converts bike lane street names to normalized Street Center Lines names
        """
        return {'AVENUE': 'AVENUE L',
                'PLLYMOUTH': 'PLYMOUTH',
                'MARTIN LUTHER KING JR': 'DR MARTIN LUTHER KING JR',
                }.get(street_name, street_name).upper()

    def process(self, df):
        for c in ['contraflow', 'br_oneway']:
            df[c] = df[c].apply(self.fix_bool)
        df['bike_ow'] = df.apply(self.bike_ow, axis=1)
        df.st_name = df.st_name.apply(self.fix_street_name)
        return df


class File:
    PROCESSORS = [BikeRouteProcessor]
    """
    Process: copy to cache, with processing step if needed
    """
    def __init__(self, filename):
        path, name = os.path.split(filename)
        root, ext = os.path.splitext(name)
        self.path = path
        self.name = name
        self.root = root
        self.ext = ext
        self.filename = filename

    def shapefile_name(self):
        return f'{self.name}.shp'

    def shapefile_dest_name(self):
        return os.path.join(DESTINATION_DIR, self.shapefile_name())

    def exists_in_cache(self):
        return os.path.exists(os.path.join(MAP_CACHE, self.name))

    def exists_in_destination(self):
        return os.path.exists(self.shapefile_dest_name())

    def process(self):
        if self.exists_in_cache():
            if self.exists_in_destination():
                print(f'Skipping {self.name}')
                return
            df = geopandas.read_file(os.path.join(MAP_CACHE, self.name))
            df2 = df.drop(columns=[x for x in df.columns if df[x].dtype.name.startswith('datetime')])
            df2.to_file(self.shapefile_dest_name())
            return
        # Load file from its location
        df = geopandas.read_file(self.filename)
        processed = False
        for p in self.PROCESSORS:
            inst = p()
            if inst.matches(self.name):
                df = inst.process(df)
                processed = True
                break
        print(f'Processed {self.filename}: {processed}')
        df.to_file(os.path.join(MAP_CACHE, self.name), driver='GeoJSON')
        df2 = df.drop(columns=[x for x in df.columns if df[x].dtype.name.startswith('datetime')])
        df2.to_file(self.shapefile_dest_name())


class Catalog:
    def __init__(self, datadir):
        self.datadir = datadir
        self.files = glob.glob(os.path.join(self.datadir, '*.geojson'))

    def process(self):
        count = 0
        for f in self.files:
            file_ = File(f)
            file_.process()
            count += 1
        print(f'Processed {count} in {self.datadir}')

if __name__ == "__main__":
    for dir_ in SOURCE_DIRS:
        c = Catalog(dir_)
        c.process()
