#!/usr/bin/env python3

import argparse
import datetime
import os

from peewee import SqliteDatabase, Model, CharField, DateField, BooleanField
import sanitize_filename

from opendatabrowser import Browser


DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'
db = SqliteDatabase(os.path.join(DESTINATION_DIR, 'fetchermetadata.sqlite3'))


class DataSource(Model):
    id_ = CharField()
    name = CharField()
    retrieved = DateField()
    fullpath = CharField()
    success = BooleanField()

    class Meta:
        database = db


class Fetcher:
    URL_TEMPLATE = 'https://data.cityofchicago.org/api/geospatial/%s?method=export&format=Shapefile'

    def __init__(self):
        self.browser = Browser()

    def fetch(self, key):
        item = self.browser.get(key)
        if not item:
            print(f'Error fetching {key}')
            return
        name = item['resource']['name']
        print(f'Fetching "{name}" / key {key}')
        type_ = item['resource'].get('type')
        if type_ != 'map':
            print(f'  Error attempting to fetch non-map')
            return
        url = self.URL_TEMPLATE % key
        fetch_time = datetime.datetime.now()
        filename = sanitize_filename.sanitize(f'{name}.zip')
        fullpath = os.path.join(DESTINATION_DIR, filename)
        print(f'fetch url {url}')
        success = True
        src = DataSource(id_=key, name=name, retrieved=fetch_time, fullpath=fullpath, success=success)
        src.save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='OpenDataFetcher',
        description='Fetch chicago open data and track metadata',
    )
    parser.add_argument('key', nargs='*')
    args = parser.parse_args()
    f = Fetcher()
    for k in args.key:
        f.fetch(k)
