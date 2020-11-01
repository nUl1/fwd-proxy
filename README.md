# FWD Proxy
FWD Proxy is a simple read-only everything-to-FTP proxy
written in python3 with asyncio and aiohttp.

It can be used e.g. to use online storage services
from programs that can use FTP and does not require a local cache.

It is not secure in any imaginable way and is not intended
for use in publicly accessible networks.

It heavily relies on somewhat correct behavior of clients
and there is next to none error handling.

It works on my machine and I have no intention on developing
it further. Pull requests and/or issues will not be watched.
