#!/usr/bin/env python3
import glob
import os
import sys
import tempfile
import zipfile

import geopandas
import fiona

fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'

if __name__ == "__main__":
    filename = sys.argv[1]
    name, ext = os.path.splitext(filename)
    zf = zipfile.ZipFile(filename)
    with tempfile.TemporaryDirectory() as tempdir:
        zf.extractall(tempdir)
        files = glob.glob(os.path.join(tempdir, '*.kml'))
        if len(files) != 1:
            print(f'Extraction error: ', files)
            sys.exit(1)
        df = geopandas.read_file(os.path.join(tempdir, files[0]))
        converted = f'{name}.geojson'
        df.to_file(converted, driver='GeoJSON')
