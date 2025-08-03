from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class Extract:
    type: str
    host: str
    port: str
    username: str
    password: str
    token: str


@dataclass
class Transfer:
    operation: Dict[str, Any]


@dataclass
class Load:
    type: str
    host: str
    port: str
    username: str
    password: str
    token: str
    bucket: str = None
    endpoint: str = None
    topic: str = None


class ETL:
    def __init__(self,
                 extracts: List[Extract],
                 transfers: List[Transfer],
                 loads: List[Load]):
        self.extracts = extracts
        self.transfers = transfers
        self.loads = loads

    def _run_extracts(self):
        pass

    def run(self):
        self._run_extracts()
        # TODO: Register for ZMQ
        # TODO: Run transfers to the data that extracted
        # TODO: Run loads to store the ouput data


class WatchETL(ETL):
    def _run_extracts(self):
        pass


class FetchETL(ETL):
    def _run_extracts(self):
        pass


class EndpointETL(ETL):
    def _run_extracts(self):
        pass
