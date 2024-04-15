#!/usr/bin/env python3

import sys

import pandas as pd
import geopandas as gpd

if __name__ == "__main__":
    filename = sys.argv[1]
    if filename.endswith('.geojson') or filename.endswith('.shp'):
        f = gpd.read_file(filename)
    elif filename.endswith('.json'):
        f = pd.read_json(filename)
    elif filename.endswith('.csv'):
        f = pd.read_csv(filename)
    else:
        print(f'Filename {filename} not recognized')
        sys.exit(1)
    print(f)
    print(f.iloc[0])
    for c in f.columns:
        vals = f[c].unique()
        if len(vals) < 15:
            print(f'Column {c}')
            for v in vals:
                print(f'  {v}')