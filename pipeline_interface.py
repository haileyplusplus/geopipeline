from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class PipelineResult:
    obj = None
    filename: str = None

    def __init__(self, obj=None, filename=None):
        self.obj = obj
        self.filename = filename

    def __str__(self):
        if self.obj is not None:
            return f'obj {self.obj}'
        elif self.filename:
            return f'fn {self.filename}'
        else:
            return 'Empty PipelineResult'

    def get(self):
        if self.obj is None:
            raise NotImplementedError
        return self.obj


class PipelineInterface(ABC):
    def __init__(self, stage_info: dict):
        """

        :type stage_info: dict
        """
        self.stage_info: dict = stage_info
        self.depend_results = {}

    @abstractmethod
    def run_stage(self) -> PipelineResult:
        pass

    def set_results(self, results):
        self.depend_results = results

    def get_dependency(self, name):
        # need to assert that dependency is actually in config
        return self.depend_results[name]