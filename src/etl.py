from typing import List, Dict, Any


class Extract:
    def __init__(self,
                 type: str,
                 source: str,
                 host: str,
                 port: str,
                 username: str,
                 password: str,
                 token: str):
        self.type = type
        self.source = source
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.token = token

    def extract(self):
        pass


class Transform:
    def __init__(self, operation: Dict[str, Any]):
        self.operation = operation

    def transform(self, data):
        pass


class Load:
    def __init__(self,
                 target: str,
                 host: str,
                 port: str,
                 username: str,
                 password: str,
                 token: str,
                 bucket: str = None,
                 endpoint: str = None,
                 topic: str = None):
        self.target = target
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.token = token
        self.bucket = bucket
        self.endpoint = endpoint
        self.topic = topic

    def load(self, data):
        pass


class ETL:
    def __init__(self,
                 extracts: List[Extract],
                 transforms: List[Transform],
                 loads: List[Load]):
        self.extracts = extracts
        self.transforms = transforms
        self.loads = loads

    def _run_extracts(self, handle_data: callable):
        pass

    def run(self):
        def handle_data(data):
            transformed_data = None
            for transform in self.transforms:
                transformed_data = {**transform.transform(data)}
            for load in self.loads:
                load.load(transformed_data)

        self._run_extracts(handle_data)
