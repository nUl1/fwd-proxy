import collections
import datetime
import logging
import pathlib

TEST_DATA = b'An implementation is left as an excercise for the reader.\n'
TEST_DATA_PATH = pathlib.PurePosixPath('/test.txt')
TEST_DATA_MTIME = datetime.datetime.utcfromtimestamp(0)

logger = logging.getLogger('datasource')


class DownloadFailed(Exception):
    pass


class DataSource:
    ListEntry = collections.namedtuple('ListEntry', ['mtime', 'size'])

    def __init__(self, session):
        # aiohttp session
        self.session = session
        self.wd = pathlib.PurePosixPath('/')

    def set_creds(self, creds):
        pass

    def chdir(self, path):
        if path == '//':
            path = '/'
        if not self.exists(path):
            return False
        self.wd /= path
        return True

    async def list(self, path=''):
        if path and path != TEST_DATA_PATH:
            return {}
        return {
            TEST_DATA_PATH.name: DataSource.ListEntry(
                mtime=TEST_DATA_MTIME,
                size=len(TEST_DATA),
            ),
        }

    def exists(self, path):
        path = self.wd / path
        return path == TEST_DATA_PATH or path == TEST_DATA_PATH.parent

    def size(self, path):
        path = self.wd / path
        return len(TEST_DATA) if path == TEST_DATA_PATH else None

    def mtime(self, path):
        path = self.wd / path
        return TEST_DATA_MTIME if path == TEST_DATA_PATH else None

    async def fetch(self, path, off):
        yield TEST_DATA
