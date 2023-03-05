from dataclasses import dataclass
from enum import Enum
from typing import List


class DiagnosisLevel(Enum):
    ERROR = 0
    WARNING =  1
    ADVICE = 2 
    OK = 3


@dataclass(init=True)
class Action:
    msg: str

    def __repr__(self) -> str:
        return self.msg


@dataclass(init=True)
class DiagnosisDetail:
    msg: str

    def __repr__(self) -> str:
        return self.msg


@dataclass(init=True)
class Diagnosis:
    name: str
    level: DiagnosisLevel
    actions: List[Action]
    details: DiagnosisDetail

    def is_ok(self):
        return self.level == DiagnosisLevel.OK 

    @classmethod 
    def OK(cls, name):
        return cls(name, level=DiagnosisLevel.OK, actions=[], details=DiagnosisDetail(msg=""))
