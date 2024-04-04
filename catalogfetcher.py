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
    retrieved = DateTimeField(null=True)
    fullpath = CharField(null=True)
    success = BooleanField(null=True)


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


class ResourceFetcher(GenericFetcher):
    URL_TEMPLATE = 'https://api.us.socrata.com/api/catalog/v1?domains=data.cityofchicago.org&limit=400&search_context=data.cityofchicago.org&categories=%s'

    def __init__(self, category, expected_count):
        url = self.URL_TEMPLATE % urllib.parse.quote_plus(category)
        super().__init__(url)
        self.category = category
        self.expected_count = expected_count

    def parse_one_resource(self, d: dict):
        r = d['resource']
        id_ = r['id']
        # check metadata date for update
        if DataSet.get_or_none(DataSet.id_ == id_):
            return False
        dataset = DataSet()
        dataset.id_ = id_
        dataset.name = r['name']
        dataset.raw = r
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
    url = 'https://data.cityofchicago.org/d/ydr8-5enu'
    #q = DataSet.select().join(Category).where(DataSet.id_ == id_)
    dataset: DataSet | None = DataSet.get_or_none(DataSet.id_ == id_)
    if not dataset:
        print(f'Couldn\'t fetch dataset {id_}')
        return None
    fetch_time = datetime.datetime.now()
    filename = sanitize_filename.sanitize(f'{dataset.name}.json')
    fullpath = os.path.join(DESTINATION_DIR, filename)
    if os.path.exists(fullpath):
        #print(f'Skipping fetch because {fullpath} exists')
        return open(fullpath).read(), dataset
    dataset.retrieved = fetch_time
    print(f'Fetching {filename}')
    # datasets available in csv or json
    r = requests.get(f'https://data.cityofchicago.org/resource/{id_}.json')
    dataset.success = r.status_code == 200
    with open(fullpath, 'w') as fh:
        fh.write(r.text)
    return r.text, dataset

# pandas join dataset series


if __name__ == "__main__":
    #gf = GenericFetcher('https://api.us.socrata.com/api/catalog/v1/domain_categories?domains=data.cityofchicago.org')
    #gf = GenericFetcher('https://api.us.socrata.com/api/catalog/v1?domains=data.cityofchicago.org&search_context=data.cityofchicago.org&categories=Public%20Safety')
    #print(f'Fetched: {gf.fetch()}')
    db.connect()
    db.create_tables([
        Category, DataSet
    ])
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
    args = parser.parse_args()
    if args.populate:
        populate_all()
    if args.s:
        q = DataSet.select().join(Category).where(DataSet.name.contains(args.s[0])).order_by(DataSet.name)
        for ds in q:
            print(f'{ds.id_}  {ds.name}')
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

