#!/usr/bin/env python3
import glob
import io
import os
import tempfile
import zipfile
import csv

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
        #dep = self.get_dependency('gtfs_fetch')
        dep = self.get_dependency_by_index(0)
        print(f'Got GTFS fetch dependency: {dep.filename}')
        # bug here - needs to work on an object as well
        print(dep)
        fn = dep.get_filename()
        assert fn is not None
        params = self.stage_info['parameters']
        feed = Feed(fn,
                    time_windows=params['time_windows'],
                    start_date=params['schedule_date'],
                    end_date=params['schedule_date'])
        line_freq = feed.lines_freq
        line_freq.head()
        rv.obj = feed
        return rv


class GTFSClean(PipelineInterface):
    @staticmethod
    def clean_file(outdir, f) -> str:
        ofn = os.path.join(outdir, f)
        with open(f) as fh:
            reader = csv.reader(fh, skipinitialspace=True)
            with open(ofn, 'w') as wfh:
                writer = csv.writer(wfh)
                for row in reader:
                    writer.writerow(row)
        return ofn

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult()
        dep = self.get_dependency_by_index(0)
        fn = dep.get_filename()
        zf = zipfile.ZipFile(fn)
        bf = io.BytesIO()
        with tempfile.TemporaryDirectory() as td:
            zf.extractall(td)
            os.chdir(td)
            with tempfile.TemporaryDirectory() as outdir:
                with zipfile.ZipFile(bf, 'w') as outzf:
                    for f in glob.glob('*.txt'):
                        _, fn = os.path.split(f)
                        ofn = self.clean_file(outdir, f)
                        outzf.write(ofn, arcname=fn)
        rv.obj = bf.getvalue()
        return rv

