#!/usr/bin/env python3

"""
 Convert a subset of the open data catalog to csv
"""

import csv
import json
import os.path
import sys

class Converter:
    RESOURCE_FIELDS = ['name', 'id', 'description', 'createdAt', 'data_updated_at',
                       'provenance']
    def __init__(self, outfile):
        self.sink = None
        self.writer = csv.DictWriter(open(outfile, 'w'), fieldnames=self.RESOURCE_FIELDS + ['domain_category'])
        self.writer.writeheader()

    def process_item(self, d):
        r = d.get('resource')
        if not r:
            return
        item = {k: str(v) for k, v in r.items() if k in self.RESOURCE_FIELDS}
        item['description'] = item['description'].replace('\r', '').replace('\n', r'\i')
        item['domain_category'] = d.get('classification', {}).get('domain_category', '')
        self.writer.writerow(item)


if __name__ == "__main__":
    if os.path.exists(sys.argv[2]):
        print('Error: file exists')
        sys.exit(1)
    converter = Converter(sys.argv[2])
    jf = json.load(open(sys.argv[1]))
    for item in jf:
        converter.process_item(item)


