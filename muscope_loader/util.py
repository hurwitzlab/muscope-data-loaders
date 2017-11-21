from contextlib import contextmanager
from itertools import zip_longest


class FileNameException(Exception):
    pass


@contextmanager
def session_(session_class):
    """Provide a transactional scope around a series of operations."""
    session = session_class()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)
