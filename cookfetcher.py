#!/usr/bin/env python3

import argparse
import datetime
import glob
import os
import json
import shutil
import subprocess
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
    retrieved = DateTimeField(null=True)
    fullpath = CharField(null=True)
    success = BooleanField(null=True)


class DataSetKeyword(BaseModel):
    keywords = ForeignKeyField(Keyword)
    dataset = ForeignKeyField(DataSet)


class Catalog:
    def __init__(self):
        self.catalog = json.load(open(CATALOG_FILE))

    def parse_dataset(self, dataset):
        filtered = {k: v for k, v in dataset.items() if k in {'identifier', 'title', 'description', 'modified'}}
        fi = filtered['identifier'].split('=')[1]
        s2 = fi.split('&')
        sublayer = False
        if len(s2) > 1 and s2[1] == 'sublayer':
            sublayer = True
        filtered['sublayer'] = 'sublayer'
        if DataSet.get_or_none(DataSet.identifier == filtered['identifier']) is not None:
            return
        ds = DataSet.create(**filtered)
        for keyword in dataset['keyword']:
            kw, created = Keyword.get_or_create(keyword=keyword)
            dsk = DataSetKeyword.create(keywords=kw, dataset=ds)
            dsk.save()

    def parse(self):
        for ds in self.catalog['dataset']:
            self.parse_dataset(ds)


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
    args = parser.parse_args()
    initialize()
    c = Catalog()
    c.parse()
    if args.summary:
        for item in DataSet.select().order_by(DataSet.title):
            print(item.title)
    if args.category:
        cat = args.category[0]
        query = DataSet.select().join(DataSetKeyword).join(Keyword).where(Keyword.keyword == cat).order_by(DataSet.title)
        for item in query:
            print(item.title)


