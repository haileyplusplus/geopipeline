#!/usr/bin/env python3

"""
https://api.us.socrata.com/api/catalog/v1?domains=data.cityofchicago.org

https://dev.socrata.com/docs/other/discovery#?route=overview
"""
import argparse
import io
import json
import os
import urllib.parse
import sys
import datetime
import sanitize_filename
from enum import Enum

import pandas as pd
import requests
from peewee import SqliteDatabase, Model, CharField, IntegerField, DateTimeField, BooleanField, TextField, ForeignKeyField



DESTINATION_DIR = '/Users/hailey/datasets/chicago'
db = SqliteDatabase(os.path.join(DESTINATION_DIR, 'fetchermetadata2.sqlite3'))


class BaseModel(Model):
    class Meta:
        database = db


class Category(BaseModel):
    name = CharField(unique=True)
    count = IntegerField()


class DataSet(BaseModel):
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
        return False


class CategoryFetcher(GenericFetcher):
    CATEGORY_URL = 'https://api.us.socrata.com/api/catalog/v1/domain_categories?domains=data.cityofchicago.org'

    def __init__(self):
        super().__init__(self.CATEGORY_URL)
        self.initialize()

    def initialize(self):
        assert self.fetch()
        for item in self.d['results']:
            _, _ = Category().get_or_create(name=item['domain_category'], count=item['count'])


class UpdateResult(Enum):
    NONE = 1
    METADATA = 2
    DATA = 3


class ResourceFetcher(GenericFetcher):
    URL_TEMPLATE = 'https://api.us.socrata.com/api/catalog/v1?domains=data.cityofchicago.org&limit=400&search_context=data.cityofchicago.org&categories=%s'

    def __init__(self, category, expected_count):
        url = self.URL_TEMPLATE % urllib.parse.quote_plus(category)
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


def populate_all():
    cf = CategoryFetcher()
    cf.initialize()
    for item in Category.select():
        rf = ResourceFetcher(item.name, item.count)
        rf.initialize()


def fetch_resource(id_):
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
    fullpath = os.path.join(DESTINATION_DIR, filename)
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
        req = requests.get(f'https://data.cityofchicago.org/api/geospatial/{id_}?method=export&format=GeoJSON')
    else:
        req = requests.get(f'https://data.cityofchicago.org/resource/{id_}.json')
    dataset.success = req.status_code == 200
    with open(fullpath, 'w') as fh:
        fh.write(req.text)
    dataset.save()
    return req.text, dataset


def db_initialize():
    db.connect()
    db.create_tables([
        Category, DataSet
    ])

# pandas join dataset series


if __name__ == "__main__":
    #gf = GenericFetcher('https://api.us.socrata.com/api/catalog/v1/domain_categories?domains=data.cityofchicago.org')
    #gf = GenericFetcher('https://api.us.socrata.com/api/catalog/v1?domains=data.cityofchicago.org&search_context=data.cityofchicago.org&categories=Public%20Safety')
    #print(f'Fetched: {gf.fetch()}')
    db_initialize()
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
    args = parser.parse_args()
    if args.pandas:
        q = DataSet.select().join(Category)
        df = pd.read_sql(q.sql()[0], db.connection())
    if args.populate:
        populate_all()
    if args.s:
        q = DataSet.select().join(Category).where(DataSet.name.contains(args.s[0])).order_by(DataSet.name)
        for ds in q:
            print(f'{ds.id_}  {ds.resource_type:12} {ds.name}')
    if args.series:
        dfs = []
        for k in args.key:
            r, dataset = fetch_resource(k)
            df = pd.read_json(io.StringIO(r))
            df['dataset'] = dataset.name
            dfs.append(df)
        combined = pd.concat(dfs, axis=0, ignore_index=True)
        print(combined['dataset'])
        combined.to_json('/tmp/combined.json')
    else:
        for k in args.key:
            r = fetch_resource(k)

        #q = DataSet.select().join(Category).where(DataSet.id_ == k)
        #for ds in q:
        #    print(ds.raw)
    #df = pd.read_sql(q.sql()[0], db.connection())

