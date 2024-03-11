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
        if not self.options.show_deprecated and result.deprecated:
            return None
        return result

    def pretty_print(self, key):
        z = self.get(key)
        if not z:
            print (f'Dataset {key} not found.')
            return
        print(json.dumps(z.d, sort_keys=True, indent=4))

    def print_summary(self, key):
        z = self.get(key)
        if not z:
            return
        print(f'{key:10} {z.name}')

    def search(self, arglist):
        text = arglist.s[0]
        for k, v in self.datasets.items():
            name = v.name
            if name.find(text) != -1:
                if arglist.summary:
                    self.print_summary(k)
                else:
                    self.pretty_print(k)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='OpenDataBrowser',
        description='Show info about open datasets',
    )
    parser.add_argument('key', nargs='*')
    parser.add_argument('-s', nargs=1, required=False)
    parser.add_argument('--summary', action='store_true')
    parser.add_argument('--show-deprecated', action='store_true')
    args = parser.parse_args()
    b = Browser(args)
    print(args)
    for k in args.key:
        if args.summary:
            b.print_summary(k)
        else:
            b.pretty_print(k)
    if args.s:
        print(f'Searching for {args.s[0]}')
        b.search(args)
