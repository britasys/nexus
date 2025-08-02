import zmq


class ZMQNode:
    def __init__(self, endpoint, context=None):
        self.context = context or zmq.Context()
        self.endpoint = endpoint
        self.socket = None

    def start(self):
        pass

    def stop(self):
        if self.socket:
            self.socket.close()

    def send(self, message):
        if self.socket:
            self.socket.send_string(message)

    def receive(self):
        if self.socket:
            return self.socket.recv_string()
        return None


class PubNode(ZMQNode):
    def start(self):
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(self.endpoint)


class SubNode(ZMQNode):
    def __init__(self, endpoint, topic="", context=None):
        super().__init__(endpoint, context)
        self.topic = topic

    def start(self):
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(self.endpoint)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)


class ReqNode(ZMQNode):
    def start(self):
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.endpoint)


class RepNode(ZMQNode):
    def start(self):
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(self.endpoint)


class PushNode(ZMQNode):
    def start(self):
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.bind(self.endpoint)


class PullNode(ZMQNode):
    def start(self):
        self.socket = self.context.socket(zmq.PULL)
        self.socket.connect(self.endpoint)
