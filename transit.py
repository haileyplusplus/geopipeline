#!/usr/bin/env python3

import requests
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
