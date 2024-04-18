#!/usr/bin/env python3

import os
import json
import importlib
import sys
from enum import Enum
from typing import List

from peewee import SqliteDatabase, Model, CharField

db = SqliteDatabase('/Users/hailey/datasets/pipeline.sqlite3')


class BaseModel(Model):
    class Meta:
        database = db


class Stage(BaseModel):
    name = CharField()


class StageExecution(BaseModel):
    name = CharField()


class WorkState(Enum):
    NOT_READY = 1
    READY = 2
    DONE = 3


class WorkContext:
    def __init__(self, stages, stage, dependencies):
        self.stages = stages
        self.stage_name = stage
        self.dependencies = dependencies
        self.state: WorkState = WorkState.NOT_READY
        self.results = None

    def set_results(self, results):
        self.results = results

    def update_state(self):
        assert self.results is not None
        if self.state == WorkState.DONE:
            return
        for d in self.dependencies:
            if not self.results.get(d):
                return
        self.state = WorkState.READY

    def process(self):
        assert self.state == WorkState.READY
        print(f'Processing {self.stage_name}')
        stage_info = self.stages[self.stage_name]
        m = stage_info.get('module')
        oc = stage_info.get('output_class')
        if m and oc:
            print(f'  Loading module {m}')
            module = importlib.import_module(m)
            inst = getattr(module, oc)(stage_info)
            inst.set_results(self.results)
            rv = inst.run_stage()
            if rv.obj is not None:
                self.results[self.stage_name] = rv
        else:
            self.results[self.stage_name] = 'results'
        self.state = WorkState.DONE


class WorkflowParser:
    def __init__(self):
        filepath = os.path.join(os.path.dirname(__file__),
                                'pipelineconfig.json')
        self.config = json.load(open(filepath))
        assert self.config.get('name') == 'pipelineconfig'
        self.stages = {}
        for d in self.config['stages']:
            self.stages[d['name']] = d
        self.workflows = {}
        for d in self.config['workflows']:
            fs = d['final']
            self.workflows[d['name']] = {
                'final': fs,
                'stages': {}
            }
            ws = self.workflows[d['name']]['stages']
            final_found = False
            assert fs in self.stages
            for s in d['stages']:
                assert s['stage'] in self.stages
                if s['stage'] == fs:
                    final_found = True
                assert s['stage'] not in ws
                for dep in s['dependencies']:
                    print(f'Verifying dependency {dep}')
                    assert dep in self.stages
                ws[s['stage']] = WorkContext(self.stages, s['stage'], s['dependencies'])
            assert final_found
        print(f'Parsed workflow config with {len(self.stages)} stages and {len(self.workflows)} workflows.')

    def get_workflow(self, workflow_name):
        return self.workflows[workflow_name]


class Runner:
    """
    Does not yet check for circular dependencies.
    """
    def __init__(self, workflow):
        self.results = {}
        self.workflow = workflow

    def debug(self):
        print(f'Debug results')
        for k, v in self.results.items():
            print(f'  {k:30}  {v}')

    def process(self):
        work_queue: List[WorkContext] = []
        work_contexts = self.workflow['stages']
        for w in work_contexts.values():
            w.set_results(self.results)
        fs = self.workflow['final']
        work_queue.append(work_contexts[fs])
        max_iters = 999
        while work_queue:
            max_iters -= 1
            item = work_queue.pop(0)
            item.update_state()
            if item.state == WorkState.DONE:
                continue
            if item.state == WorkState.READY:
                item.process()
                continue
            assert item.state == WorkState.NOT_READY
            for d in item.dependencies:
                work_queue.append(work_contexts[d])
            work_queue.append(item)
            if max_iters < 0:
                raise KeyError


if __name__ == "__main__":
    wp = WorkflowParser()
    r = Runner(wp.get_workflow('bikemap'))
    r.process()
    r.debug()
