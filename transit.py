#!/usr/bin/env python3

import requests
from gtfs_functions import Feed

from pipeline_interface import PipelineInterface, PipelineResult


class GTFSFetcher(PipelineInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        url = self.stage_info['parameters']['url']
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
        # bug here - needs to work on an object as well
        print(dep)
        assert dep.filename is not None
        feed = Feed(dep.filename, time_windows=[0, 6, 10, 16, 19, 24])
        line_freq = feed.lines_freq
        line_freq.head()
        rv.obj = feed
        return rv


class PaceFeedLoader(PipelineInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        dep = self.get_dependency('pace_gtfs_fetch')
        fn = dep.get_filename()
        print(f'Got GTFS fetch dependency: {fn}')
        assert fn is not None
        feed = Feed(fn, time_windows=[0, 6, 10, 16, 19, 24])
        line_freq = feed.lines_freq
        line_freq.head()
        rv.obj = feed
        return rv
