import tomllib

from pathlib import Path

CHICAGO_DATUM = 26916


class ConfigReader:
    def __init__(self):
        with open('config.toml', 'rb') as f:
            config = tomllib.load(f)
            self.datasets = config['datasets_path']
            self.shapefile = config['shapefile_path']
            self.pipeline_cache = config['pipeline_cache_path']


configreader = ConfigReader()


def datasets_path():
    return Path(configreader.datasets).expanduser()


def shapefile_path():
    return Path(configreader.shapefile).expanduser()


def pipeline_cache_path():
    return Path(configreader.pipeline_cache).expanduser()
