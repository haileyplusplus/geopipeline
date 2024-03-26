#!/usr/bin/env python3

import argparse
import datetime
import glob
import os
import json
import shutil
import subprocess
import sys
import tempfile
import zipfile

from peewee import SqliteDatabase, Model, CharField, DateTimeField, BooleanField, ForeignKeyField
import sanitize_filename

from opendatabrowser import Browser

"""
Next: 
 modularize this
 
 Should work for anything with this schema:
  https://project-open-data.cio.gov/v1.1/schema/catalog.json
  
  
  Fetch URL:
  https://opendata.arcgis.com/api/v3/datasets/900b69139e874c8f823744d8fd5b71eb_10/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1
   vs
  https://hub.arcgis.com/api/v3/datasets/900b69139e874c8f823744d8fd5b71eb_10/downloads/data?format=geojson&spatialRefId=4326
   vs
  https://hub-cookcountyil.opendata.arcgis.com/api/download/v1/items/900b69139e874c8f823744d8fd5b71eb/geojson?layers=10
   
"""


DESTINATION_DIR = '/Users/hailey/datasets/cook'
CATALOG_FILE = os.path.join(DESTINATION_DIR, 'index-20240325.json')
db = SqliteDatabase(os.path.join(DESTINATION_DIR, 'fetchermetadata.sqlite3'))


class BaseModel(Model):
    class Meta:
        database = db


class Keyword(BaseModel):
    keyword = CharField(primary_key=True)


class DataSet(BaseModel):
    identifier = CharField(primary_key=True)
    sublayer = BooleanField()
    title = CharField()
    description = CharField()
    modified = DateTimeField()
    geojson_url = CharField(null=True)
    retrieved = DateTimeField(null=True)
    fullpath = CharField(null=True)
    success = BooleanField(null=True)


class DataSetKeyword(BaseModel):
    keywords = ForeignKeyField(Keyword)
    dataset = ForeignKeyField(DataSet)


class Catalog:
    URL_TEMPLATE = "https://opendata.arcgis.com/api/v3/datasets/%s_10/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1"

    def __init__(self):
        self.catalog = json.load(open(CATALOG_FILE))

    def parse_dataset(self, dataset):
        filtered = {k: v for k, v in dataset.items() if k in {'identifier', 'title', 'description', 'modified'}}
        fi = filtered['identifier'].split('=')[1]
        s2 = fi.split('&')
        sublayer = False
        if len(s2) > 1 and s2[1] == 'sublayer':
            sublayer = True
        filtered['sublayer'] = sublayer
        filtered['identifier'] = s2[0]
        if DataSet.get_or_none(DataSet.identifier == filtered['identifier']) is not None:
            return
        dist = dataset.get('distribution', [])
        geojson_url = None
        for d in dist:
            if d['format'] == 'GeoJSON':
                geojson_url = d['accessURL']
                break
        if geojson_url:
            filtered['geojson_url'] = geojson_url
        ds = DataSet.create(**filtered)
        for keyword in dataset['keyword']:
            kw, created = Keyword.get_or_create(keyword=keyword)
            dsk = DataSetKeyword.create(keywords=kw, dataset=ds)
            dsk.save()

    def parse(self):
        for ds in self.catalog['dataset']:
            self.parse_dataset(ds)

    def fetch(self, key):
        item = DataSet.get(DataSet.identifier == key)
        if not item:
            print(f'Item {key} not found.')
            return False
        name = item.title
        print(f'Fetching "{name}" / key {key}')
        if item.retrieved:
            print(f' Skipping because already present.')
            return False
        return self.inner_fetch(item)

    def clear(self, key):
        item = DataSet.get(DataSet.identifier == key)
        if not item:
            print(f'Item {key} not found.')
            return False
        if item.fullpath and os.path.exists(item.fullpath):
            os.remove(item.fullpath)
        item.fullpath = None
        item.retrieved = None
        item.success = None
        item.save()
        return True


    def inner_fetch(self, item):
        #url = self.URL_TEMPLATE % item.identifier
        if not item.geojson_url:
            print(f'No geojson URL for {item.title}')
            return False
        item.retrieved = datetime.datetime.now()
        filename = sanitize_filename.sanitize(f'{item.title}.geojson')
        #fullpath = os.path.join(DESTINATION_DIR, filename)
        url = item.geojson_url
        print(f'fetch url {url}')
        fullpath = os.path.join(DESTINATION_DIR, filename)
        if os.path.exists(fullpath):
            print(f'Skipping fetch because {fullpath} exists')
            return False
        item.fullpath = fullpath
        cp = subprocess.run(['curl', '-L', '-o', fullpath, url])
        item.success = cp.returncode == 0
        item.save()
        return True

def initialize():
    db.connect()
    db.create_tables([DataSet, Keyword, DataSetKeyword])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='OpenDataBrowser',
        description='Show info about open datasets in Cook County',
    )
    parser.add_argument('key', nargs='*')
    parser.add_argument('-s', nargs=1, required=False)
    parser.add_argument('--summary', action='store_true')
    parser.add_argument('--show-deprecated', action='store_true')
    parser.add_argument('--category', nargs=1, required=False)
    parser.add_argument('--map', action='store_true')
    parser.add_argument('--clear', action='store_true', help='Clear passed-in keys.')
    args = parser.parse_args()
    initialize()
    c = Catalog()
    c.parse()
    if args.summary:
        for item in DataSet.select().order_by(DataSet.title):
            print(item.title)
    if args.map:
        query = DataSet.select().where(DataSet.geojson_url is not None)
        for item in query:
            print(f'{item.identifier:20} {item.title}')
        sys.exit(0)
    if args.category:
        cat = args.category[0]
        query = DataSet.select().join(DataSetKeyword).join(Keyword).where(Keyword.keyword == cat).order_by(DataSet.title)
        for item in query:
            print(f'{item.identifier:20} {item.title}')
        sys.exit(0)
    for k in args.key:
        if args.clear:
            result = c.clear(k)
            print (f'Cleared: {result}')
        else:
            f = c.fetch(k)
            print(f'Fetched: {f}')
