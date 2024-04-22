#!/usr/bin/env python3
import argparse
import datetime
import os
import json
import importlib
import sys
from enum import Enum
from typing import List

from peewee import SqliteDatabase, Model, CharField, DateTimeField, fn

from pipeline_interface import PipelineResult

db = SqliteDatabase('/Users/hailey/datasets/pipeline.sqlite3')


PIPELINE_STAGE_FILES = '/Users/hailey/tmp/pipelinecache'

"""
Improvements
- merge workflow stages
- no-op dep config unnecessary?
- error reporting
"""

class BaseModel(Model):
    class Meta:
        database = db


class StageExecution(BaseModel):
    name = CharField()
    executed = DateTimeField()
    status = CharField()
    stage_config = CharField()
    # relative to stage file directory
    filename = CharField()
    module_updated = DateTimeField()


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
        self.newest_dependency = None

    def set_results(self, results):
        self.results = results

    def update_state(self):
        assert self.results is not None
        if self.state == WorkState.DONE:
            return
        newest = None
        for d in self.dependencies:
            # need a better done bit; if the stage does nothing, it runs forever
            if not self.results.get(d):
                return
            updated = self.results[d].updated
            if newest is None:
                newest = updated
            else:
                newest = max(newest, updated)
        self.newest_dependency = newest
        self.state = WorkState.READY

    def process(self):
        assert self.state == WorkState.READY
        print(f'Processing {self.stage_name}')
        print(f'  newest dependency: {self.newest_dependency}')
        stage_info = self.stages[self.stage_name]
        m = stage_info.get('module')
        oc = stage_info.get('output_class')
        ot = stage_info.get('output_type')
        if m and oc:
            print(f'  Loading module {m} type {ot}')
            module = importlib.import_module(m)
            inst = getattr(module, oc)(stage_info)
            inst.set_results(self.results)
            # get stage info
            module_updated = os.stat(module.__file__).st_mtime
            previous_runs = StageExecution().select().where(StageExecution.name == self.stage_name).order_by(StageExecution.executed.desc())
            cached_filename = None
            last_executed = None
            if len(previous_runs) > 0:
                latest: StageExecution = previous_runs[0]
                stored_config = json.loads(latest.stage_config)
                freeze = stage_info.get('freeze', False)
                if module_updated > latest.module_updated and not freeze:
                    print(f'Previous execution on {latest.executed} has older module version.')
                elif self.newest_dependency and self.newest_dependency > latest.executed:
                    print(f'Previous execution on {latest.executed} depends on newer data from {self.newest_dependency}.')
                elif stored_config != stage_info:
                    print(f'Previous execution on {latest.executed} has a different stored configuration.')
                else:
                    cached_filename = os.path.join(PIPELINE_STAGE_FILES, latest.filename)
                    last_executed = latest.executed
            if cached_filename:
                # need to check whether cached data of dependencies is also too old
                # implement type-specific loader
                # also check config
                # maybe store config in a deterministic form
                print(f'Using cached result for stage {self.stage_name} from run at {last_executed}')
                rv = PipelineResult.from_cached(cached_filename, ot)
                rv.updated = last_executed
                self.results[self.stage_name] = rv
            else:
                rv = inst.run_stage()
                if not rv.empty():
                    self.results[self.stage_name] = rv
                    status = 'ok'
                else:
                    self.results[self.stage_name] = PipelineResult.as_error(f'Error running {self.stage_name}')
                    status = 'error'
                if rv.filename:
                    filename = rv.filename
                else:
                    filename = rv.serialize(PIPELINE_STAGE_FILES, ot)
                execstage = StageExecution(
                    name=self.stage_name,
                    executed=datetime.datetime.now(),
                    status=status,
                    filename=filename,
                    stage_config=json.dumps(stage_info),
                    module_updated=module_updated
                )
                execstage.save()
        else:
            self.results[self.stage_name] = PipelineResult.mark_incomplete()
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


def db_initialize():
    db.connect()
    db.create_tables([StageExecution])


def db_cleanup():
    stages = StageExecution.select(StageExecution.name, fn.MAX(StageExecution.executed)).group_by(StageExecution.name).where(StageExecution.status == 'ok')
    maxes = {}
    for s in stages:
        maxes[s.name] = s.executed
    stages = StageExecution.select()
    cleaned = 0
    for s in stages:
        if maxes.get(s.name) == s.executed:
            continue
        filename = os.path.join(PIPELINE_STAGE_FILES, s.filename)
        os.remove(filename)
        cleaned +=1
    print(f'Cleaned up {cleaned} files.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='PipelineRunner',
        description='Run processing pipeline.',
    )
    parser.add_argument('workflow_name', nargs='*')
    parser.add_argument('--cleanup', action='store_true')
    args = parser.parse_args()
    db_initialize()
    if args.cleanup:
        db_cleanup()
        sys.exit(0)
    wp = WorkflowParser()
    r = Runner(wp.get_workflow(args.workflow_name[0]))
    r.process()
    r.debug()
