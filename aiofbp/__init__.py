# coding:utf-8
import asyncio
from asyncio.queues import Queue
import logging
from uuid import uuid1

logger = logging.getLogger()


class Payload(dict):
    pass


class Port(object):
    def __init__(self, tag="data", maxsize=1, name=None, loop=None):
        loop = loop if loop is not None else asyncio.get_event_loop()
        self.loop = loop
        self.name = name if name is not None else str(uuid1())
        self._queue = Queue(maxsize, loop=self.loop)
        self.default_value = None
        self.default_value_set = False
        self.connected = False
        self.belong_to_block = None
        self.data_tag = tag

    def set_default_value(self, value):
        if not isinstance(value, Payload):
            raise Exception("value should be Payload type")
        self.default_value = value
        self.default_value_set = True

    async def get(self):
        if self.default_value_set:
            if self._queue.empty():
                return self.default_value, self.default_value[self.data_tag]

        payload = await self._queue.get()
        return payload, payload[self.data_tag]

    def get_nowait(self):
        if self.default_value_set:
            if self._queue.empty():
                return self.default_value, self.default_value[self.data_tag]

        payload = self._queue.get_nowait()
        return payload, payload[self.data_tag]

    async def put(self, payload, item):
        if self.connected:
            payload[self.data_tag] = item
            await self._queue.put(payload)

    def put_nowait(self, payload, item):
        if self.connected:
            payload[self.data_tag] = item
            self._queue.put_nowait(payload)

    def empty(self):
        return self._queue.empty()

    def full(self):
        return self._queue.full()

    def set_buffer_size(self, maxsize):
        self._queue = Queue(maxsize, loop=self.loop)


class Pipe(object):
    def __init__(self, name=None, loop=None):
        loop = loop if loop is not None else asyncio.get_event_loop()
        self.loop = loop
        self.name = name if name is not None else str(uuid1())

    def fb_connect(self, from_block, from_port, to_block, to_port):
        self.from_block = from_block
        self.from_port = from_port
        self.to_block = to_block
        self.to_port = to_port

    async def run(self):
        while 1:
            try:
                payload, val = await self.from_port.get()
                await self.to_port.put(payload, val)
            except:
                logger.error("Pipe error")


class Graph(object):
    def __init__(self, name=None, loop=None):
        loop = loop if loop is not None else asyncio.get_event_loop()
        name = name if name is not None else str(uuid1())
        self.loop = loop
        self._blocks = {}
        self._pipes = {}
        self.name = name

    def fb_connect(self, from_port, to_port, pipe_name=None):
        pipe_name = pipe_name if pipe_name is not None else str(uuid1())
        from_block = from_port.belong_to_block
        to_block = to_port.belong_to_block
        # 构建新的pipe对象
        pipe = Pipe(pipe_name, self.loop)
        pipe.fb_connect(from_block, from_port, to_block, to_port)
        from_port.connected = True
        to_port.connected = True

        # 建立Block, Pipe, Port之间的互相引用关系
        from_block.fb_connect(from_port, pipe)
        to_block.fb_connect(to_port, pipe)
        from_block.belong_to_graph = self
        to_block.belong_to_graph = self
        self._pipes[pipe_name] = pipe
        self._blocks[from_block._fb_name] = from_block
        self._blocks[to_block._fb_name] = to_block

    async def run(self):
        for block_name in self._blocks:
            block = self._blocks[block_name]
            await block.fb_before_run()
            task = block._fb_run()
            self.loop.create_task(task)

        for pipe in self._pipes:
            task = self._pipes[pipe].run()
            self.loop.create_task(task)


class BlockBase(object):
    def __init__(self, name=None, loop=None):
        loop = loop if loop is not None else asyncio.get_event_loop()
        self.loop = loop
        self._block_ports = {}
        self._port_pipe_pair = {}
        self._fb_name = name if name is not None else str(uuid1())
        self.fb_params = {}
        self.belong_to_graph = None

    def _register_port(self, port):
        self._block_ports[port.name] = port
        port.belong_to_block = self

    def fb_connect(self, port, pipe):
        self._port_pipe_pair[port.name] = (port, pipe)

    async def fb_before_run(self):
        pass

    async def fb_process(self):
        pass

    async def _fb_run(self):
        running = True
        while(running):
            try:
                await self.fb_process()
            except asyncio.CancelledError:
                running = False
            except StopBlockExecute:
                running = False
            except Exception as e:
                self._fb_log("error", str(e))

        await self.after_run()
        self._fb_log("info", "Block [%s] finished" % self._fb_name)

    async def after_run(self):
        pass

    def _fb_log(self, level, msg):
        exc_info = False
        if level == "debug":
            logger_ = logger.debug
        elif level == "info":
            logger_ = logger.info
        elif level == "error":
            logger_ = logger.error
            exc_info = True

        logger_(msg, exc_info=exc_info, extra={
            "action": self._fb_name
        })


class StopBlockExecute(Exception):
    """
    Indicate a block want to stop running. For example, a source has no more
    data and that source block should stop.
    """
    pass
