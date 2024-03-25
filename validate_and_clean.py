#!/usr/bin/env python3

import glob
import json
import os
import sys


class Validator:
    def __init__(self, path):
        self.files = glob.glob(os.path.join(path, '*.geojson'))

    @staticmethod
    def validate_file(filename):
        to_remove = False
        with open(filename) as jf:
            try:
                j = json.load(jf)
                print(f'File {filename} validated successfully')
            except json.decoder.JSONDecodeError:
                print(f'File {filename} is not valid. Removing.')
                to_remove = True
        if to_remove:
            os.remove(filename)

    def validate_all(self):
        for f in self.files:
            self.validate_file(f)


if __name__ == "__main__":
    v = Validator(sys.argv[1])
    v.validate_all()

