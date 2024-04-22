#!/usr/bin/env python3

import requests
from gtfs_functions import Feed

from pipeline_interface import PipelineInterface, PipelineResult


class GTFSFetcher(PipelineInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        url = 'https://www.transitchicago.com/downloads/sch_data/google_transit.zip'
        req = requests.get(url)
        rv.obj = req.content
        return rv


class FeedLoader(PipelineInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        dep = self.get_dependency('gtfs_fetch')
        print(f'Got GTFS fetch dependency: {dep.filename}')
        assert dep.filename is not None
        feed = Feed(dep.filename, time_windows=[0, 6, 10, 16, 19, 24])
        line_freq = feed.lines_freq
        line_freq.head()
        rv.obj = feed
        return rv
