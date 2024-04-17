#!/usr/bin/env python3

import os
import json
import sys

from peewee import SqliteDatabase, Model, CharField

db = SqliteDatabase('/Users/hailey/datasets/pipeline.sqlite3')


class BaseModel(Model):
    class Meta:
        database = db


class Stage(BaseModel):
    name = CharField()


class StageExecution(BaseModel):
    name = CharField()


class WorkflowParser:
    def __init__(self):
        filepath = os.path.join(os.path.dirname(__file__),
                                'pipelineconfig.json')
        self.config = json.load(open(filepath))
        assert self.config.get('name') == 'pipelineconfig'
        self.stages = {}
        for d in self.config['stages']:
            # placeholder
            self.stages[d['name']] = 1
        self.workflows = {}
        for d in self.config['workflows']:
            self.workflows[d['name']] = d['stages']
            fs = d['final']
            final_found = False
            assert fs in self.stages
            for s in d['stages']:
                assert s['stage'] in self.stages
                if s['stage'] == fs:
                    final_found = True
                for dep in s['dependencies']:
                    print(f'Verifying dependency {dep}')
                    assert dep in self.stages
        assert final_found
        print(f'Parsed workflow config with {len(self.stages)} stages and {len(self.workflows)} workflows.')


class Runner:
    def __init__(self):
        pass


if __name__ == "__main__":
    wp = WorkflowParser()
