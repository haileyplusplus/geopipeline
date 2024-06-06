#!/usr/bin/env python3

import os
import sys

import geopandas

from constants import datasets_path, shapefile_path


class Converter:
    def __init__(self, filename):
        path, name = os.path.split(filename)
        root, ext = os.path.splitext(name)
        self.path = path
        self.root = root
        self.ext = ext
        self.filename = filename

    def convert(self):
        print(f'Converting {self.filename}')
        if not os.path.exists(shapefile_path()):
            os.mkdir(shapefile_path())
        gf = geopandas.read_file(self.filename)
        gf.to_file(shapefile_path() / f'{self.root}.shp')


if __name__ == "__main__":
    c = Converter(sys.argv[1])
    c.convert()
