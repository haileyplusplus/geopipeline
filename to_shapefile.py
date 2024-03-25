#!/usr/bin/env python3

import os
import sys

import geopandas


class Converter:
    DATASET_DIR = '/Users/hailey/datasets/chicago'
    DESTINATION_DIR = '/Users/hailey/Documents/ArcGIS/data/chicago'

    def __init__(self, filename):
        path, name = os.path.split(filename)
        root, ext = os.path.splitext(name)
        self.path = path
        self.root = root
        self.ext = ext
        self.filename = filename

    def convert(self):
        print(f'Converting {self.filename}')
        if not os.path.exists(self.DESTINATION_DIR):
            os.mkdir(self.DESTINATION_DIR)
        gf = geopandas.read_file(self.filename)
        gf.to_file(os.path.join(self.DESTINATION_DIR, f'{self.root}.shp'))

if __name__ == "__main__":
    c = Converter(sys.argv[1])
    c.convert()
