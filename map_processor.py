#!/usr/bin/env python3

import datetime
import io
import os
import glob
import sys

from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Dict

from peewee import SqliteDatabase, Model, CharField, DateTimeField
import geopandas
import pandas as pd
import tqdm

import catalogfetcher
import cafilt

"""
 Todo:
  - fix output name to not be .geojson.shp
  - support multiple map projects, each with different processing inputs
  - support quick shapefile passthrough for map evaluation
"""

DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'
MAP_CACHE = '/Users/hailey/tmp/mapcache'
db = SqliteDatabase('/Users/hailey/datasets/processed-data.sqlite3')
SOURCE_DIRS = ['/Users/hailey/datasets/cook',
               '/Users/hailey/datasets/chicago',
               '/Users/hailey/datasets/chicago/manual']


class BaseModel(Model):
    class Meta:
        database = db


class Processing(BaseModel):
    # different dbs so no fk relationship
    name = CharField()
    process_time = DateTimeField


@dataclass(frozen=True)
class DataSource:
    domain: str
    id_: str
    filename: str


class ProcessorInterface(ABC):
    def __init__(self, managers):
        self.sources = {}
        self.name = None
        self.managers = managers

    @abstractmethod
    def process(self):
        pass

    @abstractmethod
    def get_sources(self):
        pass

    @staticmethod
    def fix_bool(x):
        if x and x.upper() == 'Y':
            return 'Y'
        return 'N'

    def fetch(self):
        name = self.__class__.__name__
        p = Processing.get_or_none(name)
        if not p:
            p = Processing()
            p.name = name
        self.name = name
        p.process_time = datetime.datetime.now()
        for s in self.get_sources():
            manager = self.managers[s.domain]
            tup = manager.fetch_resource(s.id_)
            if tup is None:
                print(f"Couldn't fetch from {s}")
                return False
            rawsource, _ = tup
            self.sources[s] = geopandas.read_file(io.StringIO(rawsource))
        return True

    def shapefile_name(self):
        return f'{self.name}.shp'

    def shapefile_dest_name(self):
        return os.path.join(DESTINATION_DIR, self.shapefile_name())


class BikeRouteProcessor(ProcessorInterface):
    def __init__(self, manager):
        super().__init__(manager)

    def get_sources(self):
        return [DataSource('chicago', '3w5d-sru8', 'Bike Routes')]

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

    def process(self):
        df = self.sources[self.get_sources()[0]]
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
    CLASS_MAP = {'1': 'expr',
                 '2': 'art',
                 '3': 'coll', # Racine, Sheffield, etc
                 '4': 'neigh',
                 '5': 'alley',
                 '7': 'tier',
                 '9': 'ramp',
                 'E': 'extent',
                 'RIV': 'river',
                 'S': 'sdwk',
                 '99': 'unk' # these appear to be mostly in O'hare
                 }

    def __init__(self, manager):
        super().__init__(manager)

    def get_sources(self):
        return [DataSource('chicago', '6imu-meau', 'Street Center Lines')]

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


# move these to different files
class BikeStreetsWrapper:
    def __init__(self, df):
        self.orig = df
        # for the Chicago area, unit in meters
        self.layer = self.orig.to_crs(epsg=26916)

    def get_buffer(self):
        return self.layer.buffer(2)

    def get_street(self, name):
        colname = 'st_name'
        if colname not in self.layer.columns:
            colname = 'street_nam'
        return self.layer[self.layer[colname] == name]

    def get_streets(self):
        colname = 'st_name'
        if colname not in self.layer.columns:
            colname = 'street_nam'
        return set(self.layer[colname].unique())


class BikeStreets(ProcessorInterface):
    def __init__(self, manager):
        super().__init__(manager)
        self.output = pd.DataFrame()
        self.streets = None
        self.bike_routes = None

    def get_sources(self):
        return [
            DataSource('chicago', '3w5d-sru8', 'Bike Routes'),
            DataSource('chicago', '6imu-meau', 'Street Center Lines'),
            #DataSource('cookgis', '900b69139e874c8f823744d8fd5b71eb', 'Off-Street Bike Trails'),
        ]

    @staticmethod
    def bike_ow(x):
        if x.contraflow == 'N' and x.br_oneway == 'Y':
            return 'Y'
        return 'N'

    @staticmethod
    def suitability(x):
        """
        Suitability index

        0 - do not display
        1 - bikes prohibited
        2 - not recommended
        3 - caution
        4 - probably light traffic, no specific infra
        5 - bike lane
        6 - greenway, buffered
        7 - protected
        8 - off-street trail
        :param x:
        :return:
        """
        if x.status != 'N':
            return 0
        cmap = {
            'RIV': 0,
            'S': 0,
            '99': 0,
            '5': 0,
            'E': 4, # try this instead
            '1': 1,
            '9': 1,
        }
        # 2, 3, 4, 7  might have bike infra
        rv = cmap.get(x['class'])
        if rv is not None:
            return rv
        dr = x.displayrou
        rmap = {
            'BIKE LANE': 5,
            'NEIGHBORHOOD GREENWAY': 6,
            'BUFFERED BIKE LANE': 6,
            'PROTECTED BIKE LANE': 7,
        }
        rv = rmap.get(dr)
        if rv is not None:
            if rv == 5 and x['class'] == '2':
                # don't allow bike lane optimization on class 2
                pass
            else:
                return rv
        if x['class'] == '4':
            return 4
        if x['class'] == '3':
            return 3
        if x['class'] == '2':
            return 2
        if x['class'] == '7':
            # consider changing this to 1
            return 1
        # shouldn't get here
        return -1


    @staticmethod
    def fix_street_name(street_name):
        """
        Converts bike lane street names to normalized Street Center Lines names
        """
        return {'AVENUE': 'AVENUE L',
                'PLLYMOUTH': 'PLYMOUTH',
                'MARTIN LUTHER KING JR': 'DR MARTIN LUTHER KING JR',
                }.get(street_name, street_name).upper()

    def merge_street(self, street_name):
        #print(f'Processing {street_name}')
        street = self.streets.get_street(street_name)
        bike_route = self.bike_routes.get_street(street_name)
        #if bike_route.empty:
        #    self.output = pd.concat([self.output, street])
        #    return
        assert not bike_route.empty
        cumulative = geopandas.GeoDataFrame()
        # street trans_id is a good unique key. we want to make sure we include segs without bike route info, too
        # 150299 is missing
        matched_segs = set([])
        for ri, route in bike_route.iterrows():
            matching = []
            nonmatching = []
            rgbuffer = route.geometry.buffer(3)
            for si, seg in street.iterrows():
                match = rgbuffer.contains(seg.geometry)
                if match:
                    matching.append(seg.drop(['create_tim', 'update_tim', 'status_dat']))
                    matched_segs.add(seg.trans_id)
            matched_frame = geopandas.GeoDataFrame(matching)
            route_frame = pd.DataFrame([route.drop('geometry')])
            if matched_frame.empty or route_frame.empty:
                #print(f'  Empty frame: {matched_frame}, {route_frame}')
                continue
            m = matched_frame.merge(route_frame, left_on='street_nam', right_on='st_name')
            #print(f'merging {m}')
            m.crs = 26916
            cumulative = pd.concat([cumulative, m])
        #print(f'cumulative with bike routes: {cumulative}')
        # now get segs without bike routes
        #return pd.concat([df, ostr[ostr.street_nam.isin(other_streets)]])
        rest_streets = street[~street.trans_id.isin(matched_segs)]
        #print(f'rest streets: {rest_streets}')
        rest_streets.crs = 26916
        cumulative = pd.concat([cumulative, rest_streets])
        #for si, seg in street.iterrows():
        #    if seg.trans_id in matched_segs:
        #        continue
        #    cumulative = cumulative.merge(seg.drop(['create_tim', 'update_tim', 'status_dat']))
        #cumulative = pd.concat([cumulative, seg.drop(['create_tim', 'update_tim', 'status_dat'])])
        #cumulative = pd.concat([cumulative, seg.drop(['create_tim', 'update_tim', 'status_dat'])], axis=0).reset_index(drop=True)
        #print(f'cumulative with all routes: {cumulative}')
        if not cumulative.empty:
            cumulative.crs = 26916
            self.output = pd.concat([self.output, cumulative])
            #self.output.append(cumulative)

    def normalize(self):
        #if not self.output:
        #    return None
        assert not self.output.empty
        #merged = pd.concat(self.output)
        self.output.crs = 26916
        #merged.crs = 26916
        #return merged.to_crs(epsg=4326)
        return self.output.to_crs(epsg=4326)

    def process(self):
        df = self.sources[self.get_sources()[0]]
        for c in ['contraflow', 'br_oneway']:
            df[c] = df[c].apply(self.fix_bool)
        df['bike_ow'] = df.apply(self.bike_ow, axis=1)
        df.st_name = df.st_name.apply(self.fix_street_name)

        self.streets = BikeStreetsWrapper(self.sources[self.get_sources()[1]])
        self.bike_routes = BikeStreetsWrapper(df)

        bike_streets = self.streets.get_streets() & self.bike_routes.get_streets()
        other_streets = self.streets.get_streets() - self.bike_routes.get_streets()
        pbar = tqdm.tqdm(bike_streets)
        for streetname in pbar:
            self.merge_street(streetname)
        #print('to merge')
        df = self.normalize()
        #print(df)
        ostr = self.streets.orig
        result = pd.concat([df, ostr[ostr.street_nam.isin(other_streets)]])
        # add one last column
        print(result)
        print(result.columns)
        result['suitability'] = result.apply(self.suitability, axis=1)
        return result


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

    def process_old(self):
        # preserve all file mode
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


class Processor:
    PROCESSORS = [BikeRouteProcessor, BikeStreets]

    def __init__(self, managers: Dict[str, catalogfetcher.Manager]):
        self.managers = managers

    # consider pbar / tqdm
    def process(self):
        for p in self.PROCESSORS:
            print(f'Running processor {p}')
            inst = p(self.managers)
            if not inst.fetch():
                return False
            gdf = inst.process()
            cache_dest = os.path.join(MAP_CACHE, f'{inst.name}.geojson')
            print(f'Write to {cache_dest}')
            print(gdf)
            gdf.to_file(cache_dest, driver='GeoJSON')
            df2 = gdf.drop(columns=[x for x in gdf.columns if gdf[x].dtype.name.startswith('datetime')])
            df2.to_file(inst.shapefile_dest_name())
            print(f'Wrote {cache_dest}')


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


def db_initialize():
    db.connect()
    db.create_tables([
        Processing
    ])


if __name__ == "__main__":
    # add override command
    managers = {}
    for d in catalogfetcher.DOMAINS:
        managers[d.name] = d.manager(d, limit=-1)
        managers[d.name].db_initialize()
    db_initialize()
    # for dir_ in SOURCE_DIRS:
    #     c = Catalog(dir_)
    #     c.process()
    p = Processor(managers)
    p.process()
    cafilt.filter_file('BikeStreets.geojson',
                       ['LINCOLN PARK', 'LAKE VIEW', 'NEAR NORTH SIDE', 'WEST TOWN'])
