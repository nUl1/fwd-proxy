import asyncio
import logging

logger = logging.getLogger('pipe')


class Pipe:
    BUF_LIMIT = 64 * 1024 * 1024

    def __init__(self, writer):
        self.writer = writer
        self.queue = asyncio.Queue()
        self.left = self.BUF_LIMIT
        self.queue_moved = None

    async def send(self, data):
        while len(data) > self.left:
            waiter = asyncio.get_running_loop().create_future()
            self.queue_moved = waiter
            await waiter
        self.left -= len(data)
        self.queue.put_nowait(data)

    async def finish(self, data=None):
        if data is not None:
            await self.send(data)
        self.queue.put_nowait(None)

    async def join(self):
        try:
            while True:
                item = await self.queue.get()
                if item is None:
                    return
                self.writer.write(item)
                if len(item) < 256 * 1024 and item.isascii():
                    logger.info(f'[D] {item}')
                else:
                    logger.info(f'[D] data chunk, {len(item)} bytes')
                await self.writer.drain()
                self.left += len(item)

                if self.queue_moved is not None:
                    self.queue_moved.set_result(None)
                    self.queue_moved = None
        finally:
            if self.queue_moved is not None:
                self.queue_moved.cancel()
                self.queue_moved = None
            self.writer.close()
            await self.writer.wait_closed()
