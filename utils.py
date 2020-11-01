import contextlib
import locale


@contextlib.contextmanager
def clocale():
    prev = locale.setlocale(locale.LC_ALL)
    try:
        locale.setlocale(locale.LC_ALL, 'C')
        yield
    finally:
        locale.setlocale(locale.LC_ALL, prev)
