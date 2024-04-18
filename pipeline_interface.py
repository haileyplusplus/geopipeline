from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class PipelineResult:
    obj = None
    filename: str = None

    def __str__(self):
        if self.obj is not None:
            return f'obj {self.obj}'
        elif self.filename:
            return f'fn {self.filename}'
        else:
            return 'Empty PipelineResult'


class PipelineInterface(ABC):
    def __init__(self, stage_info: dict):
        """

        :type stage_info: dict
        """
        self.stage_info: dict = stage_info

    @abstractmethod
    def run_stage(self) -> PipelineResult:
        pass

