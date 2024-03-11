#!/usr/bin/env python3

import argparse
import json
import sys

class Browser:
    CATALOG_FILENAME = '/Users/hailey/tmp/datacatalog-20240311.json'

    def __init__(self):
        self.datasets = {}
        z = json.load(open(self.CATALOG_FILENAME))
        for item in z:
            key = item['resource']['id']
            self.datasets[key] = item

    def get(self, key):
        return self.datasets.get(key)

    def pretty_print(self, key):
        z = self.get(key)
        if not z:
            print (f'Dataset {key} not found.')
            return
        print(json.dumps(z, sort_keys=True, indent=4))

    def print_summary(self, key):
        z = self.get(key)
        if not z:
            return
        print(f'{key:10} {z['resource']['name']}')

    def search(self, arglist):
        text = arglist.s[0]
        for k, v in self.datasets.items():
            name = v['resource']['name']
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
    parser.add_argument('--skip-deprecated', action='store_true', default=True)
    args = parser.parse_args()
    b = Browser()
    for k in args.key:
        if args.summary:
            b.print_summary(k)
        else:
            b.pretty_print(k)
    if args.s:
        print(f'Searching for {args.s[0]}')
        b.search(args)
