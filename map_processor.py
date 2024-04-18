#!/usr/bin/env python3

import datetime
import io
import os
import glob
import sys
import copy
import json

from abc import abstractmethod, ABC
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Callable

from peewee import SqliteDatabase, Model, CharField, DateTimeField
import geopandas
import shapely
import geopandas as gpd
import pandas as pd
import tqdm

import catalogfetcher
import cafilt
import constants
from pipeline_interface import PipelineInterface, PipelineResult

"""
 Todo:
  - fix output name to not be .geojson.shp
  - support multiple map projects, each with different processing inputs
  - support quick shapefile passthrough for map evaluation
  - consider supporting manual tweaks (eg connect Bloomingdale Trail to routing)
"""

DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'
MAP_CACHE = '/Users/hailey/tmp/mapcache'
# this is not currently being used
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
    filter: Callable = None
    keep_cols: frozenset[str] = frozenset()


class OverrideManager:
    def __init__(self):
        filepath = os.path.join(os.path.dirname(__file__), 'manual_overrides.json')
        self.raw_overrides = json.load(open(filepath))
        self.sources = {}
        self.initialize()

    def initialize(self):
        for ov in self.raw_overrides['overrides']:
            if not ov.keys():
                continue
            source_name = ov['source_id']['source']
            key_field = ov['source_id']['key']
            #td = {}
            self.sources[source_name] = {
                'key': key_field,
                'transformations': ov['transformations']
            }
            #for t in ov['transformations']:
            #    td[t['key']] = t['action']

    def process(self, source, df):
        transformation = self.sources.get(source)
        if not transformation:
            return df
        key = transformation['key']
        remove = []
        for t in transformation['transformations']:
            if 'key' in t:
                if t['action'] == 'remove':
                    remove.append(t['key'])
            if 'filter' in t:
                f = t['filter']
                filtercol = f['column']
                filterval = f['value']
                if 'update' in t['action']:
                    u = t['action']['update']
                    # spurious SettingWithCopyWarning but it seems to work
                    df.loc[df[filtercol] == filterval, u['column']] = u['value']
        return df[~df[key].isin(remove)]



class ProcessorInterface(ABC):
    def __init__(self, managers):
        self.sources = {}
        self.name = None
        self.managers = managers
        #self.preprocessing = {}
        self.overrides = OverrideManager()

    @abstractmethod
    def process(self):
        pass

    def get_sources(self):
        return self.SOURCES.values()

    def get_source(self, key):
        rv = self.sources[key]
        #preprocess = self.preprocessing.get(key)
        #if preprocess:
        #    return preprocess(rv)
        return self.overrides.process(key, rv)

    #def add_preprocessing_hook(self, source_key, fn):
    #    self.preprocessing[source_key] = fn

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
            df = geopandas.read_file(io.StringIO(rawsource))
            print(f'Raw {s.filename}: {df}')
            # Filter rows first since some columns might only be used for filtering rows
            if s.filter is not None:
                df = s.filter(df)
            if len(s.keep_cols) > 0:
                df = df[s.keep_cols]
            print(f'Filtered {s.filename}: {df}')
            self.sources[s.filename] = df
        return True

    def shapefile_name(self):
        return f'{self.name}.shp'

    def shapefile_dest_name(self):
        return os.path.join(DESTINATION_DIR, self.shapefile_name())


# unused
class StreetInfo:
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

    SOURCES = {
        'Street Center Lines': DataSource('chicago', '6imu-meau', 'Street Center Lines')
    }

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
    SOURCES = {
        'Bike Routes':
            DataSource(
                'chicago', '3w5d-sru8', 'Bike Routes',
                keep_cols=frozenset(['contraflow', 'br_oneway', 'displayrou', 'st_name', 'geometry']),
            ),
        'Street Center Lines':
            DataSource(
                'chicago', '6imu-meau', 'Street Center Lines',
                keep_cols=frozenset(['street_nam', 'street_typ', 'ewns_dir',
                           'dir_travel', 'status', 'class', 'length',
                           'trans_id', 'geometry']),
            ),
        'Off-Street Bike Trails':
            DataSource(
                'cookgis',
                '900b69139e874c8f823744d8fd5b71eb',
                'Off-Street Bike Trails',
                filter=lambda df: df[df.Muni == 'Chicago'][df.Status == 'Existing'],
                keep_cols=frozenset(['OBJECTID', 'Street', 'FacName', 'Sub_System', 'ShapeSTLength', 'geometry']),
            ),
    }

    def __init__(self, manager):
        super().__init__(manager)
        self.output = pd.DataFrame()
        self.streets = None
        self.bike_routes = None

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
            'OFF STREET': 8, # this is fictionally postprocessed
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
                    #matching.append(seg.drop(['create_tim', 'update_tim', 'status_dat']))
                    matching.append(seg)
                    matched_segs.add(seg.trans_id)
            matched_frame = geopandas.GeoDataFrame(matching)
            route_frame = pd.DataFrame([route.drop('geometry')])
            if matched_frame.empty or route_frame.empty:
                #print(f'  Empty frame: {matched_frame}, {route_frame}')
                continue
            m = matched_frame.merge(route_frame, left_on='street_nam', right_on='st_name')
            #print(f'merging {m}')
            m.crs = constants.CHICAGO_DATUM
            cumulative = pd.concat([cumulative, m])
        #print(f'cumulative with bike routes: {cumulative}')
        # now get segs without bike routes
        #return pd.concat([df, ostr[ostr.street_nam.isin(other_streets)]])
        rest_streets = street[~street.trans_id.isin(matched_segs)]
        #print(f'rest streets: {rest_streets}')
        rest_streets.crs = constants.CHICAGO_DATUM
        cumulative = pd.concat([cumulative, rest_streets])
        #for si, seg in street.iterrows():
        #    if seg.trans_id in matched_segs:
        #        continue
        #    cumulative = cumulative.merge(seg.drop(['create_tim', 'update_tim', 'status_dat']))
        #cumulative = pd.concat([cumulative, seg.drop(['create_tim', 'update_tim', 'status_dat'])])
        #cumulative = pd.concat([cumulative, seg.drop(['create_tim', 'update_tim', 'status_dat'])], axis=0).reset_index(drop=True)
        #print(f'cumulative with all routes: {cumulative}')
        if not cumulative.empty:
            cumulative.crs = constants.CHICAGO_DATUM
            self.output = pd.concat([self.output, cumulative])
            #self.output.append(cumulative)

    def normalize(self):
        # incompletely working on processing everything in consistent coord systems
        #if not self.output:
        #    return None
        assert not self.output.empty
        #merged = pd.concat(self.output)
        self.output.crs = constants.CHICAGO_DATUM
        self.output['actual'] = self.output.apply(lambda x: x.geometry.length, axis=1)
        self.output['suitability'] = self.output.apply(self.suitability, axis=1)

        suitability_multipliers = {
            2: 5.0,
            3: 2.0,
            4: 0.9,
            5: 1,
            6: 0.75,
            7: 0.6,
            8: 0.5,
        }

        def weightfn(attrs):
            weight = attrs['actual']
            mult = suitability_multipliers.get(attrs['suitability'], 0)
            # print(f'Path weight: {weight} m{mult} E{e0} {e1} {raw}')
            if mult == 0:
                return 1000000000
            return mult * weight

        self.output['weight'] = self.output.apply(weightfn, axis=1)
        #merged.crs = constants.CHICAGO_DATUM
        #return merged.to_crs(epsg=4326)
        return self.output.to_crs(epsg=4326)

    @staticmethod
    def dfline(gdf):
        """
        Splits all linestrings into 2-seg strings

        May want to try to interpolate intersecting vertices

        :param gdf:
        :return:
        """
        df = gpd.GeoDataFrame()
        df.crs = 4326
        for _, f in gdf.iterrows():
            t = f.to_dict()
            g = f.geometry
            assert g.geom_type == 'LineString'
            prev = None
            for i, c in enumerate(g.coords):
                if prev is not None:
                    t['si'] = i
                    t['geometry'] = shapely.LineString([prev, c])
                    df = pd.concat([df, gpd.GeoDataFrame([t])])
                prev = c
        return df

    @staticmethod
    def get_dicts(rd):
        g = rd['geometry']
        tid = rd['trans_id']
        assert g.geom_type == 'LineString'
        prev = None
        count = 0
        for i, c in enumerate(g.coords):
            count += 1
            if prev is not None:
                rd['trans_id'] = f'{tid}-{count}'
                rd['geometry'] = shapely.LineString([prev, c])
                rd['actual'] = rd['geometry'].length
                yield rd
            prev = c

    def preprocess_off_street(self):
        off_street: geopandas.GeoDataFrame = self.get_source('Off-Street Bike Trails')
        print(f'Off street {off_street}')
        dds = []
        off_street = off_street.to_crs(constants.CHICAGO_DATUM)
        for _, row in off_street.iterrows():
            if row.geometry.geom_type != 'LineString':
                continue
            id_ = row.FacName
            if not id_:
                id_ = row.Street
            if not id_:
                id_ = row.Sub_System
            #rca = gpd.GeoDataFrame([row])
            #rca.crs = 4326
            #rc = rca.to_crs(constants.CHICAGO_DATUM)
            rd = {'length': row.ShapeSTLength,
                  'status': 'N',
                  'dir_travel': 'B',
                  'street_nam': id_,
                  'trans_id': f'B{row.OBJECTID}',
                  'geometry': row.geometry,
                  'street_typ': '',
                  'ewns_dir': '',
                  'class': '4',
                  'displayrou': 'OFF STREET',
                  'st_name': id_,
                  'br_oneway': 'N',
                  'contraflow': 'N',
                  'bike_ow': False,
                  'actual': row.geometry.length}
            if row.geometry.length > 200:
                for d2 in self.get_dicts(rd):
                    dds.append(copy.copy(d2))
            else:
                dds.append(copy.copy(rd))
        offdf = geopandas.GeoDataFrame(dds)
        offdf.crs = constants.CHICAGO_DATUM
        #return offdf.to_crs(4326)
        return offdf

    def process(self):
        df = self.get_source('Bike Routes')
        print(f'Processing with {df} src 0 (bike routes)')
        for c in ['contraflow', 'br_oneway']:
            df[c] = df[c].apply(self.fix_bool)
        df['bike_ow'] = df.apply(self.bike_ow, axis=1)
        df.st_name = df.st_name.apply(self.fix_street_name)

        centerlines = self.get_source('Street Center Lines')
        centerlines['trans_id'] = centerlines['trans_id'].apply(lambda x: f'A{x}')
        self.streets = BikeStreetsWrapper(centerlines)
        self.bike_routes = BikeStreetsWrapper(df)

        bike_streets = self.streets.get_streets() & self.bike_routes.get_streets()
        other_streets = self.streets.get_streets() - self.bike_routes.get_streets()
        pbar = tqdm.tqdm(bike_streets)
        for streetname in pbar:
            self.merge_street(streetname)
        #print('to merge')
        #df = self.normalize()
        #print(df)
        ostr = self.streets.layer
        #print(f'ostr {ostr}')
        #result = pd.concat([df, ostr[ostr.street_nam.isin(other_streets)]])
        #result = pd.concat([df, ostr[ostr.street_nam.isin(other_streets)]])
        self.output = pd.concat([self.output, ostr[ostr.street_nam.isin(other_streets)]])

        # Now add in off-street routes using the column format above
        offdf = self.preprocess_off_street()
        self.output = pd.concat([self.output, offdf], ignore_index=True)
        result = self.normalize()
        # add one last column
        print(result)
        print(result.columns)
        return result


class StreetsPreprocess(PipelineInterface):
    def __init__(self, stage_info):
        super().__init__(stage_info)

    def run_stage(self) -> PipelineResult:
        centerlines = self.get_dependency('streets_fetch').get()
        centerlines['trans_id'] = centerlines['trans_id'].apply(lambda x: f'A{x}')
        rv = PipelineResult()
        rv.obj = centerlines
        return rv


class BikeRoutesPreprocess(PipelineInterface):
    def __init__(self, stage_info):
        super().__init__(stage_info)

    @staticmethod
    def fix_bool(x):
        if x and x.upper() == 'Y':
            return 'Y'
        return 'N'

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

    def run_stage(self) -> PipelineResult:
        df = self.get_dependency('bike_routes_fetch').get()
        print(f'Processing with {df} src 0 (bike routes)')
        for c in ['contraflow', 'br_oneway']:
            df[c] = df[c].apply(self.fix_bool)
        df['bike_ow'] = df.apply(self.bike_ow, axis=1)
        df.st_name = df.st_name.apply(self.fix_street_name)
        return PipelineResult(obj=df)


class OffStreetPreprocess(PipelineInterface):
    def __init__(self, stage_info):
        super().__init__(stage_info)

    @staticmethod
    def get_dicts(rd):
        g = rd['geometry']
        tid = rd['trans_id']
        assert g.geom_type == 'LineString'
        prev = None
        count = 0
        for i, c in enumerate(g.coords):
            count += 1
            if prev is not None:
                rd['trans_id'] = f'{tid}-{count}'
                rd['geometry'] = shapely.LineString([prev, c])
                rd['actual'] = rd['geometry'].length
                yield rd
            prev = c


    def run_stage(self) -> PipelineResult:
        off_street: geopandas.GeoDataFrame = self.get_dependency('off_street_fetch').get()
        print(f'Off street {off_street}')
        dds = []
        off_street = off_street.to_crs(constants.CHICAGO_DATUM)
        for _, row in off_street.iterrows():
            if row.geometry.geom_type != 'LineString':
                continue
            id_ = row.FacName
            if not id_:
                id_ = row.Street
            if not id_:
                id_ = row.Sub_System
            #rca = gpd.GeoDataFrame([row])
            #rca.crs = 4326
            #rc = rca.to_crs(constants.CHICAGO_DATUM)
            rd = {'length': row.ShapeSTLength,
                  'status': 'N',
                  'dir_travel': 'B',
                  'street_nam': id_,
                  'trans_id': f'B{row.OBJECTID}',
                  'geometry': row.geometry,
                  'street_typ': '',
                  'ewns_dir': '',
                  'class': '4',
                  'displayrou': 'OFF STREET',
                  'st_name': id_,
                  'br_oneway': 'N',
                  'contraflow': 'N',
                  'bike_ow': False,
                  'actual': row.geometry.length}
            if row.geometry.length > 200:
                for d2 in self.get_dicts(rd):
                    dds.append(copy.copy(d2))
            else:
                dds.append(copy.copy(rd))
        offdf = geopandas.GeoDataFrame(dds)
        offdf.crs = constants.CHICAGO_DATUM
        #return offdf.to_crs(4326)
        rv = PipelineResult()
        return PipelineResult(obj=offdf)


class StreetsBikeJoin(PipelineInterface):
    def __init__(self, stage_info):
        super().__init__(stage_info)
        self.output = pd.DataFrame()

    def merge_street(self, street_name):
        street = self.streets.get_street(street_name)
        bike_route = self.bike_routes.get_street(street_name)
        assert not bike_route.empty
        cumulative = geopandas.GeoDataFrame()
        # street trans_id is a good unique key. we want to make sure we include segs without bike route info, too
        # 150299 is missing
        matched_segs = set([])
        for ri, route in bike_route.iterrows():
            matching = []
            rgbuffer = route.geometry.buffer(3)
            for si, seg in street.iterrows():
                match = rgbuffer.contains(seg.geometry)
                if match:
                    matching.append(seg)
                    matched_segs.add(seg.trans_id)
            matched_frame = geopandas.GeoDataFrame(matching)
            route_frame = pd.DataFrame([route.drop('geometry')])
            if matched_frame.empty or route_frame.empty:
                continue
            m = matched_frame.merge(route_frame, left_on='street_nam', right_on='st_name')
            m.crs = constants.CHICAGO_DATUM
            cumulative = pd.concat([cumulative, m])
        # now get segs without bike routes
        rest_streets = street[~street.trans_id.isin(matched_segs)]
        rest_streets.crs = constants.CHICAGO_DATUM
        cumulative = pd.concat([cumulative, rest_streets])
        if not cumulative.empty:
            cumulative.crs = constants.CHICAGO_DATUM
            self.output = pd.concat([self.output, cumulative])

    def run_stage(self) -> PipelineResult:
        self.streets = BikeStreetsWrapper(self.get_dependency('streets_preprocess').get())
        self.bike_routes = BikeStreetsWrapper(self.get_dependency('bike_routes_preprocess').get())
        bike_streets = self.streets.get_streets() & self.bike_routes.get_streets()
        other_streets = self.streets.get_streets() - self.bike_routes.get_streets()
        pbar = tqdm.tqdm(bike_streets)
        for streetname in pbar:
            self.merge_street(streetname)
        ostr = self.streets.layer
        rv = PipelineResult()
        rv.obj = pd.concat([self.output, ostr[ostr.street_nam.isin(other_streets)]])
        return rv


class BikeStreetsOffJoin(PipelineInterface):
    def __init__(self, stage_info):
        super().__init__(stage_info)
        self.rv = PipelineResult()
        self.output = self.rv.obj

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
            'OFF STREET': 8, # this is fictionally postprocessed
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

    def normalize(self):
        # incompletely working on processing everything in consistent coord systems
        # if not self.output:
        #    return None
        assert not self.output.empty
        # merged = pd.concat(self.output)
        self.output.crs = constants.CHICAGO_DATUM
        self.output['actual'] = self.output.apply(lambda x: x.geometry.length, axis=1)
        self.output['suitability'] = self.output.apply(self.suitability, axis=1)

        suitability_multipliers = {
            2: 5.0,
            3: 2.0,
            4: 0.9,
            5: 1,
            6: 0.75,
            7: 0.6,
            8: 0.5,
        }

        def weightfn(attrs):
            weight = attrs['actual']
            mult = suitability_multipliers.get(attrs['suitability'], 0)
            # print(f'Path weight: {weight} m{mult} E{e0} {e1} {raw}')
            if mult == 0:
                return 1000000000
            return mult * weight

        self.output['weight'] = self.output.apply(weightfn, axis=1)
        # merged.crs = constants.CHICAGO_DATUM
        # return merged.to_crs(epsg=4326)
        return self.output.to_crs(epsg=4326)

    def run_stage(self) -> PipelineResult:
        # Now add in off-street routes using the column format above
        offdf = self.get_dependency('off_street_preprocess').get()
        bikejoin = self.get_dependency('streets_bike_join').get()
        self.output = pd.concat([bikejoin, offdf], ignore_index=True)
        result = self.normalize()
        self.rv.obj = result
        return self.rv


class Processor:
    #PROCESSORS = [BikeRouteProcessor, BikeStreets]
    PROCESSORS = [BikeStreets]

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
            dropped = [x for x in gdf.columns if gdf[x].dtype.name.startswith('datetime')]
            if dropped:
                df2 = gdf.drop(columns=dropped)
            else:
                df2 = gdf
            df2.to_file(inst.shapefile_dest_name())
            print(f'Wrote {cache_dest}')


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
                       ['LINCOLN PARK', 'LAKE VIEW', 'NEAR NORTH SIDE', 'WEST TOWN', 'LOGAN SQUARE'])
