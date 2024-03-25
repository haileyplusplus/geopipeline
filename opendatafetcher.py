#!/usr/bin/env python3

import argparse
import datetime
import glob
import os
import shutil
import subprocess
import tempfile
import zipfile

from peewee import SqliteDatabase, Model, CharField, DateTimeField, BooleanField
import sanitize_filename

from opendatabrowser import Browser

"""
Next: 
 extract automatically and give better names
 explore arcgis db tables in sqlite
"""

DESTINATION_DIR = '/Users/hailey/datasets/chicago'
db = SqliteDatabase(os.path.join(DESTINATION_DIR, 'fetchermetadata.sqlite3'))


class DataSet(Model):
    id_ = CharField()
    name = CharField()
    retrieved = DateTimeField()
    fullpath = CharField()
    success = BooleanField()

    class Meta:
        database = db


class DataSetInstance:
    URL_TEMPLATE = 'https://data.cityofchicago.org/api/geospatial/%s?method=export&format=GeoJSON'

    def __init__(self, key):
        self.key = key
        self.fullpath = None
        self.name = None

    def fetch(self, browser):
        key = self.key
        item = browser.get(key)
        if not item:
            print(f'Error fetching {key}')
            return False
        name = item.name
        print(f'Fetching "{name}" / key {key}')
        type_ = item.type_
        if type_ != 'map':
            print(f'  Error attempting to fetch non-map')
            return False
        self.inner_fetch(name)
        return True

    def inner_fetch(self, name):
        self.name = name
        url = self.URL_TEMPLATE % self.key
        fetch_time = datetime.datetime.now()
        filename = sanitize_filename.sanitize(f'{name}.geojson')
        fullpath = os.path.join(DESTINATION_DIR, filename)
        print(f'fetch url {url}')
        self.fullpath = fullpath
        if os.path.exists(fullpath):
            print(f'Skipping fetch because {fullpath} exists')
            return
        cp = subprocess.run(['curl', '-o', fullpath, url])
        success = cp.returncode == 0
        src = DataSet(id_=self.key, name=name, retrieved=fetch_time, fullpath=fullpath, success=success)
        src.save()


class Fetcher:
    def __init__(self):
        self.browser = Browser(None)
        self.datasets = []

    def add(self, key):
        self.datasets.append(DataSetInstance(key))

    def process_all(self, force):
        for d in self.datasets:
            fetched = d.fetch(self.browser)
            if not fetched and force:
                d.inner_fetch(k)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='OpenDataFetcher',
        description='Fetch chicago open data and track metadata',
    )
    parser.add_argument('key', nargs='*')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--input_file', nargs=1, required=False)
    args = parser.parse_args()
    db.connect()
    db.create_tables([DataSet])
    f = Fetcher()
    if args.input_file:
        lines = [x.strip() for x in open(args.input_file[0]).readlines() if x.strip()]
        for line in lines:
            f.add(line)
    for k in args.key:
        f.add(k)
    f.process_all(force=args.force)
