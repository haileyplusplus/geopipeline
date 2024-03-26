#!/usr/bin/env python3

import sys

import geopandas

if __name__ == "__main__":
    filename = sys.argv[1]
    f = geopandas.read_file(filename)
    print(f)
    print(f.iloc[0])
    for c in f.columns:
        vals = f[c].unique()
        if len(vals) < 15:
            print(f'Column {c}')
            for v in vals:
                print(f'  {v}')