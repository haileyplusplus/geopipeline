import os

from pathlib import Path

CHICAGO_DATUM = 26916


def datasets_path():
    return Path(os.getenv('DATASETS_DIR'))


def shapefile_path():
    return Path('/Users/hailey/Documents/ArcGIS/data/chicago')


def pipeline_cache_path():
    return Path('/Users/hailey/tmp/pipelinecache')
