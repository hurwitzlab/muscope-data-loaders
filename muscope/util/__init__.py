from itertools import islice, zip_longest


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))


class FileNameException(Exception):
    pass
