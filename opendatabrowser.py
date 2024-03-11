#!/usr/bin/env python3

import argparse
import json
import sys


class DataFeed:
    """
    Represents a single data feed from Chicago's open data site. These may need to be aggregated.
    """
    def __init__(self, d):
        self.d = d
        self.r = d['resource']
        self.id_ = self.r['id']
        self.name = self.r['name']
        self.description = self.r['description']
        self.domain_tags = frozenset(d['classification'].get('domain_tags', []))
        self.deprecated = 'deprecated' in self.domain_tags
        self.type_ = self.r['type']
        self.category = d['classification'].get('domain_category', '')

    def __str__(self):
        return f'{self.id_:10} {self.name}'

    def __lt__(self, o):
        return self.name < o.name


class Browser:
    CATALOG_FILENAME = '/Users/hailey/tmp/datacatalog-20240311.json'

    def __init__(self, options):
        self.options = options
        self.datasets = {}
        z = json.load(open(self.CATALOG_FILENAME))
        for item in z:
            key = item['resource']['id']
            self.datasets[key] = DataFeed(item)

    def get(self, key):
        result = self.datasets.get(key)
        if not result:
            return result
        if result.deprecated:
            if self.options and not self.options.show_deprecated:
                return None
        if self.options:
            if self.options.category and self.options.category[0] != result.category:
                return None
            if self.options.map and result.type_ != 'map':
                return None
        return result

    def pretty_print(self, key):
        z = self.get(key)
        if not z:
            print (f'Dataset {key} not found.')
            return
        print(json.dumps(z.d, sort_keys=True, indent=4))

    def get_summary(self, key):
        z = self.get(key)
        if not z:
            return
        return f'{key:10} {z.name}'

    def search(self, arglist):
        text = arglist.s[0]
        summaries = []
        for k, v in sorted(self.datasets.items()):
            name = v.name
            if text == '*' or name.find(text) != -1:
                if arglist.summary:
                    summaries.append(v)
                else:
                    self.pretty_print(k)
        if summaries:
            summaries.sort()
            for x in summaries:
                if self.get(x.id_):
                    print(x)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='OpenDataBrowser',
        description='Show info about open datasets',
    )
    parser.add_argument('key', nargs='*')
    parser.add_argument('-s', nargs=1, required=False)
    parser.add_argument('--summary', action='store_true')
    parser.add_argument('--show-deprecated', action='store_true')
    parser.add_argument('--category', nargs=1, required=False)
    parser.add_argument('--map', action='store_true')
    args = parser.parse_args()
    b = Browser(args)
    print(args)
    for k in args.key:
        if args.summary:
            print(b.get_summary(k))
        else:
            b.pretty_print(k)
    if args.s:
        print(f'Searching for {args.s[0]}')
        b.search(args)
