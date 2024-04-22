#!/usr/bin/env python3

import requests
from gtfs_functions import Feed

from pipeline_interface import PipelineInterface, PipelineResult


class TransitMerge(PipelineInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_stage(self) -> PipelineResult:
        rv = PipelineResult.mark_incomplete()
        return rv

