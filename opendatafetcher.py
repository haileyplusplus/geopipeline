#!/usr/bin/env python3

import argparse
import datetime
import os
import subprocess

from peewee import SqliteDatabase, Model, CharField, DateTimeField, BooleanField
import sanitize_filename

from opendatabrowser import Browser


DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'
db = SqliteDatabase(os.path.join(DESTINATION_DIR, 'fetchermetadata.sqlite3'))


class DataSource(Model):
    id_ = CharField()
    name = CharField()
    retrieved = DateTimeField()
    fullpath = CharField()
    success = BooleanField()

    class Meta:
        database = db


class Fetcher:
    URL_TEMPLATE = 'https://data.cityofchicago.org/api/geospatial/%s?method=export&format=Shapefile'

    def __init__(self, args):
        self.browser = Browser(args)

    def fetch(self, key):
        item = self.browser.get(key)
        if not item:
            print(f'Error fetching {key}')
            return
        name = item.name
        print(f'Fetching "{name}" / key {key}')
        type_ = item.type_
        if type_ != 'map':
            print(f'  Error attempting to fetch non-map')
            return
        url = self.URL_TEMPLATE % key
        fetch_time = datetime.datetime.now()
        filename = sanitize_filename.sanitize(f'{name}.zip')
        fullpath = os.path.join(DESTINATION_DIR, filename)
        print(f'fetch url {url}')
        cp = subprocess.run(['curl', '-o', fullpath, url])
        success = cp.returncode == 0
        src = DataSource(id_=key, name=name, retrieved=fetch_time, fullpath=fullpath, success=success)
        src.save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='OpenDataFetcher',
        description='Fetch chicago open data and track metadata',
    )
    parser.add_argument('key', nargs='*')
    parser.add_argument('--summary', action='store_true')
    parser.add_argument('--show-deprecated', action='store_true')
    args = parser.parse_args()
    db.connect()
    db.create_tables([DataSource])
    f = Fetcher(args)
    for k in args.key:
        f.fetch(k)
