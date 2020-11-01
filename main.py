import aiohttp
import argparse
import asyncio
import logging
import socket

import datasource
import ftpsession

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(name)s:%(message)s'
)

HOST = None
HTTP_SESSION = None


async def ftp_handler(reader, writer):
    session = ftpsession.FtpSession(
        HOST,
        reader,
        writer,
        datasource.DataSource(HTTP_SESSION)
    )
    try:
        try:
            await session.dispatch()
        finally:
            await session.close()
    except ftpsession.DropSession:
        pass
    except BrokenPipeError:
        pass
    except ConnectionResetError:
        pass


async def main():
    global HOST
    global HTTP_SESSION

    parser = argparse.ArgumentParser()
    parser.add_argument('host', help='IPv4 address for binding')
    parser.add_argument('port', type=int, help='Command channel port number')
    args = parser.parse_args()

    HOST = args.host

    async with aiohttp.ClientSession() as client:
        HTTP_SESSION = client
        server = await asyncio.start_server(
            ftp_handler,
            HOST,
            port=args.port,
            family=socket.AF_INET
        )
        async with server:
            await server.serve_forever()

asyncio.run(main())
