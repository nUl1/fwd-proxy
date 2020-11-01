import asyncio
import datetime
import logging
import socket

import pipe
import utils

logger = logging.getLogger('ftpsession')


class DropSession(Exception):
    pass


class FtpSession:
    def __init__(self, host, reader, writer, datasource):
        self.host = host
        self.cmd_reader = reader
        self.cmd_writer = writer

        self.pasv_server = None
        self.pasv_port = None
        self.pasv_established = None
        self.transfer = None
        self.rest = None

        self.datasource = datasource

    async def _reply(self, msg):
        logger.info(f'[>] {msg}')
        if isinstance(msg, list):
            msg = '\r\n'.join(msg)
        msg += '\r\n'
        self.cmd_writer.write(msg.encode())
        await self.cmd_writer.drain()

    async def _ensure_pasv_server(self):
        if self.pasv_server is not None:
            return

        async def pasv_handler(reader, writer):
            self.pasv_established.set_result(writer)

        self.pasv_server = await asyncio.start_server(
            pasv_handler,
            host=self.host,
            port=0,
            family=socket.AF_INET
        )
        for s in self.pasv_server.sockets:
            if s.family != socket.AF_INET:
                continue
            self.pasv_port = s.getsockname()[1]
            break
        else:
            raise Exception('Cannot find passive server port')

    async def close(self):
        self.cmd_writer.close()
        await self.cmd_writer.wait_closed()
        if self.pasv_server is not None:
            self.pasv_server.close()
            await self.pasv_server.wait_closed()
        if self.transfer is not None:
            self.transfer.cancel()
            await self.transfer

    async def _list(self, conn):
        DIR_HEADER = 'dr-xr-xr-x 2 fwd fwd 4096'
        FILE_HEADER = '-r--r--r-- 2 fwd fwd'

        now = datetime.datetime.utcnow()

        def fmt_time(t):
            with utils.clocale():
                if t.year == now.year:
                    return t.strftime('%b %e %H:%M')
                else:
                    return t.strftime('%b %e %Y')

        entries = await self.datasource.list()
        listing = ''
        for name, entry in entries.items():
            if entry.size is None:
                listing += ' '.join([DIR_HEADER, fmt_time(entry.mtime), name])
            else:
                listing += ' '.join([
                    FILE_HEADER,
                    str(entry.size),
                    fmt_time(entry.mtime),
                    name,
                ])
            listing += '\r\n'

        sender = pipe.Pipe(conn)
        await sender.finish(listing.encode())
        await sender.join()
        return True

    async def _retr(self, conn, path):
        sender = pipe.Pipe(conn)
        send_task = asyncio.create_task(sender.join())

        try:
            try:
                async for chunk in self.datasource.fetch(path, self.rest):
                    await sender.send(chunk)
            finally:
                await sender.finish()
                await send_task
        except Exception:
            return False
        finally:
            self.rest = None
        return True

    async def dispatch(self):
        await self._reply('220 nUl1 FWD Server')

        while True:
            if self.transfer is not None:
                # asyncio.wait looks better but does not cancel on timeout
                if self.transfer.done():
                    if await self.transfer:
                        await self._reply('226 Transfer done')
                    else:
                        await self._reply('426 Transfer failed')
                    self.transfer = None
                try:
                    line = await asyncio.wait_for(
                        self.cmd_reader.readline(),
                        timeout=1
                    )
                except asyncio.TimeoutError:
                    continue
            else:
                line = await self.cmd_reader.readline()
            line = line.rstrip().decode()
            if not line:
                break

            logger.info(f'[<] {line}')
            cmd, *args = line.split(maxsplit=1)
            cmd = cmd.lower()
            args = args[0] if args else ''

            if cmd == 'user':
                if args == 'fwd':
                    await self._reply('331 Okay, give me your fancy string')
                else:
                    await self._reply('530 Not today')
            elif cmd == 'pass':
                self.datasource.set_creds(args)
                await self._reply('230 Whatever')
            elif cmd == 'syst':
                await self._reply('215 UNIX')
            elif cmd == 'feat':
                await self._reply([
                    '211-Features:',
                    ' EPSV',
                    ' MDTM',
                    ' REST STREAM',
                    ' SIZE',
                    ' TVFS',
                    ' UTF8',
                    '211 End'
                ])
            elif cmd == 'opts':
                args = args.split()
                if not args:
                    await self._reply('501 Option required')
                elif args[0].lower() == 'utf8':
                    if len(args) < 2 or args[1].lower() == 'on':
                        await self._reply('200 Always on')
                    else:
                        await self._reply('501 Always on')
                else:
                    await self._reply('501 Unknown option')
            elif cmd == 'pwd':
                await self._reply(f'257 "{self.datasource.wd}"')
            elif cmd == 'type':
                if args == 'I':
                    await self._reply('200 OK')
                else:
                    await self._reply('504 Unsupported type')
            elif cmd == 'cwd':
                if self.datasource.chdir(args):
                    await self._reply(f'200 "{self.datasource.wd}"')
                else:
                    await self._reply('550 Unknown')
            elif cmd == 'pasv':
                await self._ensure_pasv_server()
                self.pasv_established = (
                    asyncio.get_running_loop().create_future()
                )
                await self._reply(
                    '227 Passive ready ('
                    f'{self.host.replace(".",",")},'
                    f'{self.pasv_port // 256},{self.pasv_port % 256}'
                    ')'
                )
            elif cmd == 'epsv':
                await self._ensure_pasv_server()
                self.pasv_established = (
                    asyncio.get_running_loop().create_future()
                )
                await self._reply(f'229 Passive ready (|||{self.pasv_port}|)')
            elif cmd == 'list':
                conn = await self.pasv_established
                self.transfer = asyncio.create_task(self._list(conn))
                await self._reply('150 Listing')
            elif cmd == 'size':
                ret = self.datasource.size(args)
                if ret is not None:
                    await self._reply(f'213 {ret}')
                else:
                    await self._reply('550 Unknown')
            elif cmd == 'mdtm':
                ret = self.datasource.mtime(args)
                if ret is not None:
                    await self._reply(f'213 {ret.strftime("%Y%m%d%H%M%S")}')
                else:
                    await self._reply('550 Unknown')
            elif cmd == 'rest':
                self.rest = int(args)
                await self._reply('350 Duly noted')
            elif cmd == 'retr':
                if self.datasource.exists(args):
                    conn = await self.pasv_established
                    self.transfer = asyncio.create_task(self._retr(conn, args))
                    await self._reply('150 Starting transfer')
                else:
                    await self._reply('550 Unknown')
            elif cmd == 'abor':
                if self.transfer is not None:
                    self.transfer.cancel()
                    if await self.transfer:
                        await self._reply('226 Transfer done')
                    else:
                        await self._reply('426 Transfer aborted')
                    self.transfer = None
                await self._reply('226 No active transfer')
            elif cmd == 'quit':
                self.cmd_writer.close()
                if self.transfer is not None:
                    await self.transfer
                    break
            else:
                await self._reply('502 Not implemented')
                raise DropSession()
