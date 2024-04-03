#!/usr/bin/env python3

"""
https://api.us.socrata.com/api/catalog/v1?domains=data.cityofchicago.org

https://dev.socrata.com/docs/other/discovery#?route=overview
"""

import json
import os

import requests
from peewee import SqliteDatabase, Model, CharField, IntegerField


DESTINATION_DIR = '/Users/hailey/datasets/chicago'
db = SqliteDatabase(os.path.join(DESTINATION_DIR, 'fetchermetadata2.sqlite3'))


class BaseModel(Model):
    class Meta:
        database = db


class Category(BaseModel):
    name = CharField(unique=True)
    count = IntegerField()


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


if __name__ == "__main__":
    #gf = GenericFetcher('https://api.us.socrata.com/api/catalog/v1/domain_categories?domains=data.cityofchicago.org')
    #gf = GenericFetcher('https://api.us.socrata.com/api/catalog/v1?domains=data.cityofchicago.org&search_context=data.cityofchicago.org&categories=Public%20Safety')
    #print(f'Fetched: {gf.fetch()}')
    db.connect()
    db.create_tables([
        Category
    ])
    cf = CategoryFetcher()
    cf.initialize()