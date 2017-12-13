import json
import urllib.parse

import requests

from druiddb import exceptions


apilevel = '2.0'
# Threads may share the module and connections
threadsafety = 3
paramstyle = 'pyformat'


def connect(host='localhost', port=8082, path='/druid/v2/sql/', scheme='http'):
    return Connection(host, port, path, scheme)


class Connection:

    def __init__(
        self,
        host='localhost',
        port=8082,
        path='/druid/v2/sql/',
        scheme='http',
    ):
        netloc = f'{host}:{port}'
        self.url = urllib.parse.urlunparse(
            (scheme, netloc, path, None, None, None))

    def close(self):
        pass

    def commit(self):
        pass

    def cursor(self):
        return Cursor(self.url)


class Cursor:

    def __init__(self, url):
        self.url = url

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # these are updated only after a query
        self.description = None
        self.rowcount = -1

        self._results = None

    def close(self):
        self.closed = True

    def execute(self, operation, parameters=None):
        if parameters:
            raise exceptions.NotSupportedError('Parameters are not supported')

        self._results = self._stream_query(operation)

    def executemany(self, operation, seq_of_parameters=None):
        pass

    def fetchone(self):
        return self.next()

    def fetchmany(self, size=None):
        size = size or self.arraysize
        return [self.next() for _ in range(size)]

    def fetchall(self):
        return list(self)

    def setinputsizes(self, sizes):
        pass

    def setoutputsizes(self, sizes):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._results)

    next = __next__

    def _stream_query(self, query):
        """
        Stream rows from a query.

        This method will yield rows as the data is returned in chunks from the
        server.
        """
        headers = {'Content-Type': 'application/json'}
        payload = {'query': query}
        r = requests.post(self.url, stream=True, headers=headers, json=payload)

        if r.encoding is None:
            r.encoding = 'utf-8'

        # Druid will stream the data in chunks of 8k bytes, splitting the JSON
        # between them; setting `chunk_size` to `None` makes it use the server
        # size
        body = ''
        for block in r.iter_content(chunk_size=None, decode_unicode=True):
            if block:
                body += block

            # find last complete row
            boundary = 0
            brackets = 0
            in_string = False
            for i, char in enumerate(body):
                if char == '"':
                    if not in_string:
                        in_string = True
                    elif body[i - 1] != '\\':
                        in_string = False

                if in_string:
                    continue

                if char == '{':
                    brackets += 1
                elif char == '}':
                    brackets -= 1
                    if brackets == 0 and i > boundary:
                        boundary = i + 1

            rows = body[:boundary].lstrip('[,')
            body = body[boundary:]

            for row in json.loads(f'[{rows}]'):
                yield tuple(row.values())


if __name__ == '__main__':
    conn = connect('localhost')
    curs = conn.cursor()
    curs.execute(
        "SELECT status, region FROM rides WHERE status != 'canceled' LIMIT 10")
    for row in curs:
        print(row)
    curs.execute(
        "SELECT status, region FROM rides WHERE status != 'canceled' LIMIT 10")
    print(curs.fetchone())
    print('line')
    print(curs.fetchall())
