from typing import List, Dict, Any
import json

from zmq_w import ZMQPubNode, ZMQSubNode


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

        self.extracts_pub_queue = ZMQPubNode(
            "tcp://localhost:5555")
        self.transforms_sub_queue = ZMQSubNode(
            "tcp://localhost:5555", "extract")
        self.transforms_pub_queue = ZMQPubNode(
            "tcp://localhost:5556")
        self.loads_sub_queue = ZMQSubNode(
            "tcp://localhost:5556", "load")

        self.extracts_pub_queue.start()
        self.transforms_sub_queue.start()
        self.transforms_pub_queue.start()
        self.loads_sub_queue.start()

    def _run_extracts(self):
        data = {
            "temperature": 24,
            "humidity": 60,
            "unit": "C"
        }

        topic = "weather"
        json_msg = json.dumps(data)
        self.extracts_pub_queue.send(f"{topic} {json_msg}")

    def _run_transforms(self):
        data = self.transforms_sub_queue.receive()
        self.transforms_pub_queue.send(data)

    def _run_loads(self):
        data = self.loads_sub_queue.receive()

    def run(self):
        self._run_extracts()
        self._run_transforms()
        self._run_loads()
