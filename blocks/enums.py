from enum import Enum


class WorkerType(Enum):
    BLOCK = 'BLOCK'
    TX_PRIME = 'TX_PRIME'
    TX_DETAIL = 'TX_DETAIL'

    def __str__(self):
        return self.name

    @classmethod
    def from_string(cls, v):
        return cls.__members__.get(v)
