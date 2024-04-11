#!/usr/bin/env python3

"""
https://api.us.socrata.com/api/catalog/v1?domains=data.cityofchicago.org

datacatalog.cookcountyil.gov

https://dev.socrata.com/docs/other/discovery#?route=overview


Improvements
- chunked data and progress bar
- limits and updating
- better parsing of success status

"""
import argparse
import io
import json
import os
import urllib.parse
import sys
import datetime
from dataclasses import dataclass

import sanitize_filename
from enum import Enum

import pandas as pd
import requests
from peewee import SqliteDatabase, Model, CharField, IntegerField, DateTimeField, BooleanField, TextField, ForeignKeyField


@dataclass
class CatalogInfo:
    name: str
    destination_dir: str
    domain: str


db = SqliteDatabase(None)
# make this a dict
DOMAINS = [
    CatalogInfo('chicago', '/Users/hailey/datasets/chicago', 'data.cityofchicago.org'),
    CatalogInfo('cook', '/Users/hailey/datasets/cook', 'datacatalog.cookcountyil.gov'),
]


class BaseModel(Model):
    class Meta:
        database = db


class Category(BaseModel):
    name = CharField(unique=True)
    count = IntegerField()


class DataSet(BaseModel):
    # add sha256
    id_ = CharField(unique=True)
    name = CharField()
    description = TextField()
    resource_type = CharField()
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


class CategoryFetcher(GenericFetcher):
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


class ResourceFetcher(GenericFetcher):
    def __init__(self, catalog: CatalogInfo, category, expected_count):
        self.catalog = catalog
        cat = urllib.parse.quote_plus(category)
        d = self.catalog.domain
        url = f'https://api.us.socrata.com/api/catalog/v1?domains={d}&limit=400&search_context={d}&categories={cat}'
        super().__init__(url)
        self.category = category
        self.expected_count = expected_count

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

    def parse_one_resource(self, d: dict):
        r = d['resource']
        id_ = r['id']
        # check metadata date for update
        dataset = DataSet()
        dataset.id_ = id_
        dataset.name = r['name']
        dataset.raw = json.dumps(d)
        dataset.description = r['description']
        dataset.resource_type = r['type']
        dataset.updated = r['updatedAt']
        dataset.created = r['createdAt']
        dataset.metadata_updated = r['metadata_updated_at']
        dataset.data_updated = r['data_updated_at']
        c = d['classification']
        dmd = {}
        for dd in c['domain_metadata']:
            dmd[dd['key']] = dd['value']
        dataset.metadata_frequency = dmd.get('Metadata_Frequency')
        dataset.metadata_owner = dmd.get('Metadata_Data-Owner')
        dataset.metadata_period = dmd.get('Metadata_Time-Period')
        if c['domain_category'] != self.category:
            print(f'Category mismatch in item {id_}')
        dataset.category = Category.get(Category.name == self.category)
        previous = DataSet.get_or_none(DataSet.id_ == id_)
        if previous:
            ur = self.check_newer(dataset, previous)
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
                print(f'  Previous {self.last_update(previous)} New {self.last_update(dataset)}')
        dataset.save()
        return True

    def initialize(self):
        assert self.fetch()
        results = self.d['results']
        parsed = 0
        updated = 0
        for item in results:
            update = self.parse_one_resource(item)
            parsed += 1
            if update:
                updated += 1
        if parsed != self.expected_count:
            print(f'Error processing {self.category}: Expected {self.expected_count} but got {parsed}')
        print(f'Parsed {self.category}: {parsed} items, {updated} updated')


class Manager:
    def __init__(self, catalog: CatalogInfo, limit: int):
        self.catalog = catalog
        self.limit = limit

    def populate_all(self):
        cf = CategoryFetcher(self.catalog)
        cf.initialize()
        for item in Category.select():
            rf = ResourceFetcher(self.catalog, item.name, item.count)
            rf.initialize()

    def fetch_resource(self, id_):
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

    def db_initialize(self):
        db.init(os.path.join(self.catalog.destination_dir, 'fetchermetadata2.sqlite3'))
        db.connect()
        db.create_tables([
            Category, DataSet
        ])

# pandas join dataset series


if __name__ == "__main__":
    #gf = GenericFetcher('https://api.us.socrata.com/api/catalog/v1/domain_categories?domains=data.cityofchicago.org')
    #gf = GenericFetcher('https://api.us.socrata.com/api/catalog/v1?domains=data.cityofchicago.org&search_context=data.cityofchicago.org&categories=Public%20Safety')
    #print(f'Fetched: {gf.fetch()}')
    parser = argparse.ArgumentParser(
        prog='OpenDataBrowser',
        description='Show info about open datasets',
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
    parser.add_argument('--domain', nargs=1, required=False, default='chicago')
    parser.add_argument('--dump', action='store_true')
    parser.add_argument('--limit', nargs=1, type=int, default=200000000)
    args = parser.parse_args()
    catalog = None
    for d in DOMAINS:
        if d.name == args.domain[0]:
            catalog = d
            break
    if catalog is None:
        print(f'Domain {args.domain} not found.')
        sys.exit(1)
    print(f'Using domain {args.domain}')
    m = Manager(catalog, args.limit[0])
    m.db_initialize()
    if args.pandas:
        q = DataSet.select().join(Category)
        df = pd.read_sql(q.sql()[0], db.connection())
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

