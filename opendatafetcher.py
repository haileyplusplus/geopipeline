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

DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'
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
    URL_TEMPLATE = 'https://data.cityofchicago.org/api/geospatial/%s?method=export&format=Shapefile'

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
        filename = sanitize_filename.sanitize(f'{name}.zip')
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

    def extract(self):
        if not self.fullpath:
            return
        zf = zipfile.ZipFile(self.fullpath)
        with tempfile.TemporaryDirectory() as tempdir:
            zf.extractall(tempdir)
            files = glob.glob(os.path.join(tempdir, '*'))
            filenames = set([os.path.splitext(fn)[0] for fn in files])
            if len(filenames) != 1:
                print(f'Archive {self.name} did not have the expected contents.')
                return
            fullfilename = next(iter(filenames))
            _, filename = os.path.split(fullfilename)
            safename = sanitize_filename.sanitize(self.name)
            for f in files:
                path, name = os.path.split(f)
                newname = name.replace(filename, safename)
                if newname == name:
                    print(f'Failure replacing {name}: {filename}, {safename}')
                    continue
                destfullpath = os.path.join(DESTINATION_DIR, newname)
                if os.path.exists(destfullpath):
                    print(f'Skipping copy to {destfullpath}')
                    continue
                shutil.move(f, destfullpath)


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
            d.extract()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='OpenDataFetcher',
        description='Fetch chicago open data and track metadata',
    )
    parser.add_argument('key', nargs='*')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    db.connect()
    db.create_tables([DataSet])
    f = Fetcher()
    for k in args.key:
        f.add(k)
    f.process_all(force=args.force)
