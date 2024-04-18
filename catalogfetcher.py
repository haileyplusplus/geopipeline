#!/usr/bin/env python3

"""
https://api.us.socrata.com/api/catalog/v1?domains=data.cityofchicago.org

datacatalog.cookcountyil.gov

https://dev.socrata.com/docs/other/discovery#?route=overview


Improvements
- chunked data and progress bar
- limits and updating
  - filter Cook Co data to Chicago townships
- better parsing of success status
    eg status: pending
- fix pseudo-geojson (thegeometry column)
- infer data series over time, use metadata to coalesce same set
- read column metadata and make accessible
"""
import argparse
import io
import json
import os
import urllib.parse
import sys
import datetime
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Callable
import geopandas

import sanitize_filename
from enum import Enum

import pandas as pd
import requests
from peewee import SqliteDatabase, Model, CharField, IntegerField, DateTimeField, BooleanField, TextField, ForeignKeyField, DatabaseProxy

from interfaces import ManagerInterface
from pipeline_interface import PipelineInterface, PipelineResult


@dataclass
class CatalogInfo:
    name: str
    destination_dir: str
    domain: str
    manager: type


#db = SqliteDatabase(None)
database_proxy = DatabaseProxy()

class BaseModel(Model):
    class Meta:
        database = database_proxy


class Category(BaseModel):
    name = CharField(unique=True)
    count = IntegerField()


class DataSet(BaseModel):
    # add sha256
    id_ = CharField(unique=True)
    name = CharField()
    description = TextField()
    resource_type = CharField()
    sha256 = CharField(null=True)
    url = CharField(null=True)
    updated = DateTimeField(null=True)
    created = DateTimeField(null=True)
    metadata_updated = DateTimeField(null=True)
    data_updated = DateTimeField(null=True)
    metadata_frequency = CharField(null=True)
    metadata_owner = CharField(null=True)
    metadata_period = CharField(null=True)
    category = ForeignKeyField(Category)
    raw = TextField()
    # These are internal properties; maybe they should be a different object
    retrieved = DateTimeField(null=True)
    fullpath = CharField(null=True)
    success = BooleanField(null=True)
    data_stale = BooleanField(null=True)


class GenericFetcher:
    def __init__(self, api_endpoint):
        self.api_endpoint = api_endpoint
        self.d = {}
        self.raw = None

    def fetch(self):
        r = requests.get(self.api_endpoint)
        self.raw = r
        if r.status_code != 200:
            print(f'Received status {r.status_code} for {self.api_endpoint}')
            return False
        if r.headers['content-type'].startswith('application/json'):
            self.d = r.json()
            return True
        print(r.headers)
        print(r.text)
        return False


class CategoryIndexFetcher(GenericFetcher):
    # https://datacatalog.cookcountyil.gov/

    def __init__(self, catalog: CatalogInfo):
        self.catalog = catalog
        url = f'https://api.us.socrata.com/api/catalog/v1/domain_categories?domains={self.catalog.domain}'
        super().__init__(url)
        self.initialize()

    def initialize(self):
        assert self.fetch()
        for item in self.d['results']:
            category, created = Category().get_or_create(name=item['domain_category'])
            if not created and category.count != item['count']:
                print(f'Item count update for {category.name}: was {category.count}, now {item['count']}')
            category.count = item['count']
            category.save()


class UpdateResult(Enum):
    NONE = 1
    METADATA = 2
    DATA = 3


class CategoryFetcher2(GenericFetcher):
    def __init__(self, domain, category, expected_count):
        cat = urllib.parse.quote_plus(category)
        url = f'https://api.us.socrata.com/api/catalog/v1?domains={domain}&limit=400&search_context={domain}&categories={cat}'
        super().__init__(url)
        self.category = category
        self.expected_count = expected_count

    def produce_dataset(self, rd: dict) -> DataSet:
        r = rd['resource']
        id_ = r['id']
        # check metadata date for update
        dataset = DataSet()
        dataset.id_ = id_
        dataset.name = r['name']
        dataset.raw = json.dumps(rd)
        dataset.description = r['description']
        dataset.resource_type = r['type']
        dataset.updated = r['updatedAt']
        dataset.created = r['createdAt']
        dataset.metadata_updated = r['metadata_updated_at']
        dataset.data_updated = r['data_updated_at']
        c = rd['classification']
        dmd = {}
        for dd in c['domain_metadata']:
            dmd[dd['key']] = dd['value']
        dataset.metadata_frequency = dmd.get('Metadata_Frequency')
        dataset.metadata_owner = dmd.get('Metadata_Data-Owner')
        dataset.metadata_period = dmd.get('Metadata_Time-Period')
        if c['domain_category'] != self.category:
            print(f'Category mismatch in item {id_}')
        dataset.category = Category.get(Category.name == self.category)
        return dataset

    def populate_category(self, producer: Callable[[DataSet], bool]):
        assert self.fetch()
        results = self.d['results']
        parsed = 0
        updated = 0
        for item in results:
            dataset3 = self.produce_dataset(item)
            update = producer(dataset3)
            parsed += 1
            if update:
                updated += 1
        if parsed != self.expected_count:
            print(f'Error processing {self.category}: Expected {self.expected_count} but got {parsed}')
        print(f'Parsed {self.category}: {parsed} items, {updated} updated')


class ManagerBase(ManagerInterface):
    def __init__(self, catalog2: CatalogInfo, limit: int):
        self.catalog = catalog2
        self.limit = limit
        self.mydb: SqliteDatabase = self.db_initialize()

    @abstractmethod
    def populate_all(self):
        pass

    @abstractmethod
    def fetch_resource(self, id_):
        pass

    @staticmethod
    def check_newer(ds: DataSet, other: DataSet) -> UpdateResult:
        """
        :param ds:
        :param other:
        :return: Whether ds is newer than other
        """
        newer = lambda t1, t2: t1 and t2 and (t1 > t2)
        ur: UpdateResult = UpdateResult.NONE
        if newer(ds.metadata_updated, other.metadata_updated):
            ur = UpdateResult.METADATA
        if newer(ds.updated, other.updated) or newer(ds.data_updated, other.data_updated):
            ur = UpdateResult.DATA
        return ur

    @staticmethod
    def last_update(ds):
        #d = datetime.datetime.fromtimestamp(0)
        d = ''
        if ds.updated:
            #if not type(ds.updated) is datetime.datetime:
            #    print(f'Assertion error: {ds.name}, {ds.updated}')
            d = max(d, ds.updated)
        if ds.metadata_updated:
            d = max(d, ds.metadata_updated)
        if ds.data_updated:
            d = max(d, ds.data_updated)
        return d

    @staticmethod
    def parse_one_resource(dataset: DataSet) -> bool:
        previous = DataSet.get_or_none(DataSet.id_ == dataset.id_)
        if previous:
            ur = ManagerBase.check_newer(dataset, previous)
            if ur == UpdateResult.NONE:
                return False
            dataset.retrieved = previous.retrieved
            dataset.success = previous.success
            dataset.fullpath = previous.fullpath
            stale = False
            if ur == UpdateResult.DATA and previous.retrieved:
                dataset.data_stale = True
                stale = True
            dataset.id = previous.id
            if ur == UpdateResult.METADATA:
                print(f'Metadata-only update for {dataset.name}')
            if ur == UpdateResult.DATA:
                print(f'Data update for {dataset.name}. Existing stale: {stale}')
                print(f'  Previous {ManagerBase.last_update(previous)} New {ManagerBase.last_update(dataset)}')
        dataset.save()
        return True

    def db_initialize(self):
        dbpath = os.path.join(self.catalog.destination_dir, 'fetchermetadata2.sqlite3')
        db = SqliteDatabase(dbpath)
        print(f'Loading {dbpath}')
        #db.init(dbpath)
        database_proxy.initialize(db)
        db.connect()
        db.create_tables([
            Category, DataSet
        ])
        #print(f'Initialized {db} in {self.catalog.name}')
        return db

    def rebind(self):
        #print(f'Rebinding db was: {DataSet._meta.database}')
        self.mydb.bind([Category, DataSet])
        #print(f'Now bound to: {DataSet._meta.database}')


class Manager(ManagerBase):
    def __init__(self, catalog: CatalogInfo, limit: int):
        super().__init__(catalog, limit)

    def fetch_category(self, category, expected_count):
        cat = urllib.parse.quote_plus(category)
        domain = self.catalog.domain
        url = f'https://api.us.socrata.com/api/catalog/v1?domains={domain}&limit=400&search_context={domain}&categories={cat}'
        #super().__init__(url)
        #self.category = category
        #self.expected_count = expected_count


    def populate_all(self):
        cf = CategoryIndexFetcher(self.catalog)
        cf.initialize()
        for item in Category.select():
            rf = CategoryFetcher2(self.catalog.domain, item.name, item.count)
            rf.populate_category(ManagerBase.parse_one_resource)

    def fetch_resource(self, id_):
        self.rebind()
        dataset: DataSet | None = DataSet.get_or_none(DataSet.id_ == id_)
        if not dataset:
            print(f'Couldn\'t fetch dataset {id_}')
            return None
        fetch_time = datetime.datetime.now()
        ext = 'json'
        map = dataset.resource_type == 'map'
        if map:
            ext = 'geojson'
        filename = sanitize_filename.sanitize(f'{dataset.name}.{ext}')
        fullpath = os.path.join(self.catalog.destination_dir, filename)
        if os.path.exists(fullpath):
            print(f'Skipping fetch because {fullpath} exists')
            print(f'Retrieved: {dataset.retrieved}')
            print(f'Stale: {dataset.data_stale}')
            print(f'Updated: {dataset.updated}')
            print(f'Data updated: {dataset.data_updated}')
            print(f'Metadata updated: {dataset.metadata_updated}')
            return open(fullpath).read(), dataset
        dataset.retrieved = fetch_time
        # heuristic: mistrust updated too close to retrieved time?
        print(f'Fetching {filename}')
        # datasets available in csv or json
        if map:
            url = f'https://{self.catalog.domain}/api/geospatial/{id_}?method=export&format=GeoJSON'
        else:
            url = f'https://{self.catalog.domain}/resource/{id_}.json?$limit={self.limit}'
        print(f'Fetching {url}')
        req = requests.get(url)
        dataset.success = req.status_code == 200
        print(f'dataset: {dataset.success}')
        if len(req.text) < 500:
            print(f'short text: {req.text}')
        print(len(req.content))
        print(req.headers)
        print(f'json: {len(req.json())}')
        with open(fullpath, 'w') as fh:
            fh.write(req.text)
        dataset.save()
        return req.text, dataset


class CookGISManager(ManagerBase):
    def __init__(self, catalog: CatalogInfo, limit: int):
        super().__init__(catalog, limit)
        self.data_catalog = {}

    @staticmethod
    def parse_dataset(dsdict) -> DataSet:
        category, _ = Category().get_or_create(name='CookGIS', count=-1)
        filtered = {k: v for k, v in dsdict.items() if k in {'identifier', 'title', 'description', 'modified', 'issued'}}
        fi = filtered['identifier'].split('=')[1]
        s2 = fi.split('&')
        sublayer = False
        if len(s2) > 1 and s2[1] == 'sublayer':
            sublayer = True
        filtered['sublayer'] = sublayer
        filtered['identifier'] = s2[0]
        dist = dsdict.get('distribution', [])
        geojson_url = None
        for dd in dist:
            if dd['format'] == 'GeoJSON':
                geojson_url = dd['accessURL']
                break
        if geojson_url:
            filtered['geojson_url'] = geojson_url
        rv = DataSet()
        rv.id_ = filtered['identifier']
        rv.name = filtered['title']
        rv.description = filtered['description']
        rv.resource_type = 'map'
        rv.updated = filtered['modified']
        rv.category = category
        rv.raw = json.dumps(dsdict)
        issued = filtered.get('issued')
        if issued:
            rv.created = issued
        if geojson_url:
            rv.url = geojson_url
        assert rv is not None
        return rv

    def populate_all(self):
        gf = GenericFetcher('https://hub-cookcountyil.opendata.arcgis.com/api/feed/dcat-us/1.1.json')
        if not gf.fetch():
            return False
        self.data_catalog = gf.d
        for dataset2 in self.data_catalog['dataset']:
            ds2 = self.parse_dataset(dataset2)
            assert ds2 is not None
            ManagerBase.parse_one_resource(ds2)
        return True

    # need to refactor and combine this
    def fetch_resource(self, id_):
        self.rebind()
        dataset: DataSet | None = DataSet.get_or_none(DataSet.id_ == id_)
        if not dataset:
            print(f'Couldn\'t fetch dataset {id_}')
            return None
        fetch_time = datetime.datetime.now()
        ext = 'json'
        map = dataset.resource_type == 'map'
        if map:
            ext = 'geojson'
        filename = sanitize_filename.sanitize(f'{dataset.name}.{ext}')
        fullpath = os.path.join(self.catalog.destination_dir, filename)
        if os.path.exists(fullpath):
            print(f'Skipping fetch because {fullpath} exists')
            print(f'Retrieved: {dataset.retrieved}')
            print(f'Stale: {dataset.data_stale}')
            print(f'Updated: {dataset.updated}')
            print(f'Data updated: {dataset.data_updated}')
            print(f'Metadata updated: {dataset.metadata_updated}')
            return open(fullpath).read(), dataset
        dataset.retrieved = fetch_time
        # heuristic: mistrust updated too close to retrieved time?
        print(f'Fetching {filename}')
        # datasets available in csv or json
        if not dataset.url:
            print(f'No URL for dataset')
            return None
        url = dataset.url
        print(f'Fetching {url}')
        req = requests.get(url)
        dataset.success = req.status_code == 200
        print(f'dataset: {dataset.success}')
        if len(req.text) < 500:
            print(f'short text: {req.text}')
        print(len(req.content))
        print(req.headers)
        print(f'json: {len(req.json())}')
        with open(fullpath, 'w') as fh:
            fh.write(req.text)
        dataset.save()
        return req.text, dataset

# pandas join dataset series


# make this a dict
DOMAINS = {
    'chicago': CatalogInfo('chicago', '/Users/hailey/datasets/chicago', 'data.cityofchicago.org', Manager),
    'cook': CatalogInfo('cook', '/Users/hailey/datasets/cook', 'datacatalog.cookcountyil.gov', Manager),
    'cookgis': CatalogInfo('cookgis', '/Users/hailey/datasets/cookgis', None, CookGISManager),
}


class PipelineFetcher(PipelineInterface):
    def __init__(self, stage_info):
        super().__init__(stage_info)

    def run_stage(self) -> PipelineResult:
        limit = 10000000
        rv = PipelineResult()
        ds = self.stage_info['parameters']['datasource']
        #cataloginfo = CatalogInfo(name=ds['name'], destdir, )
        cataloginfo = DOMAINS[ds['domain']]
        mm = cataloginfo.manager(cataloginfo, limit)
        mm.db_initialize()
        tup = mm.fetch_resource(ds['feed_id'])
        rawsource, _ = tup
        rv.obj = geopandas.read_file(io.StringIO(rawsource))
        # need to do filtering
        return rv


if __name__ == "__main__":
    #gf = GenericFetcher('https://api.us.socrata.com/api/catalog/v1/domain_categories?domains=data.cityofchicago.org')
    #gf = GenericFetcher('https://api.us.socrata.com/api/catalog/v1?domains=data.cityofchicago.org&search_context=data.cityofchicago.org&categories=Public%20Safety')
    #print(f'Fetched: {gf.fetch()}')
    parser = argparse.ArgumentParser(
        prog='CatalogFetcher',
        description='Fetch data catalogs and data',
    )
    parser.add_argument('key', nargs='*')
    parser.add_argument('-s', nargs=1, required=False)
    parser.add_argument('--summary', action='store_true')
    parser.add_argument('--show-deprecated', action='store_true')
    parser.add_argument('--populate', action='store_true')
    parser.add_argument('--series', action='store_true')
    parser.add_argument('--category', nargs=1, required=False)
    parser.add_argument('--map', action='store_true')
    parser.add_argument('--pandas', action='store_true')
    parser.add_argument('--domain', nargs=1, required=False, default=['chicago'])
    parser.add_argument('--dump', action='store_true')
    parser.add_argument('--limit', nargs=1, type=int, default=[200000000])
    args = parser.parse_args()
    catalog = None
    # really?
    for d in DOMAINS.values():
        if d.name == args.domain[0].strip():
            catalog = d
            break
    if catalog is None:
        print(f'Domain {args.domain} not found.')
        sys.exit(1)
    print(f'Using domain {args.domain}')
    #m = CookGISManager(catalog, args.limit[0])
    m = catalog.manager(catalog, args.limit[0])
    m.db_initialize()
    if args.pandas:
        q = DataSet.select().join(Category)
        df = pd.read_sql(q.sql()[0], m.mydb.connection())
    if args.populate:
        m.populate_all()
    if args.s:
        q = DataSet.select().join(Category).where(DataSet.name.contains(args.s[0])).order_by(DataSet.name)
        for ds in q:
            print(f'{ds.id_}  {ds.resource_type:12} {ds.name}')
    if args.dump:
        q = DataSet.select().join(Category).order_by(DataSet.name)
        for ds in q:
            print(f'{ds.id_}  {ds.resource_type:12} {ds.name}')
    if args.series:
        dfs = []
        for k in args.key:
            r, dataset = m.fetch_resource(k)
            df = pd.read_json(io.StringIO(r))
            df['dataset'] = dataset.name
            dfs.append(df)
        combined = pd.concat(dfs, axis=0, ignore_index=True)
        print(combined['dataset'])
        combined.to_json('/tmp/combined.json')
    else:
        for k in args.key:
            r, ds = m.fetch_resource(k)
            if ds.resource_type == 'map':
                import geopandas
                gdf = geopandas.read_file(io.StringIO(r))

        #q = DataSet.select().join(Category).where(DataSet.id_ == k)
        #for ds in q:
        #    print(ds.raw)
    #df = pd.read_sql(q.sql()[0], db.connection())

