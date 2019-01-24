from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple
import itertools
import json

from dbapihelper import Type
from dbapihelper.connection import Connection
from dbapihelper.cursor import IteratorCursor
from dbapihelper.exceptions import Error, ProgrammingError
import requests
from six import string_types
from six.moves.urllib import parse


def connect(host='localhost', port=8082, path='/druid/v2/sql/', scheme='http'):
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 8082)
        >>> curs = conn.cursor()

    """
    netloc = f'{host}:{port}'
    url = parse.urlunparse((scheme, netloc, path, None, None, None))
    return DruidConnection(url=url)


def get_description_from_row(row):
    """
    Return description from a single row.

    We only return the name, type (inferred from the data) and if the values
    can be NULL. String columns in Druid are NULLable. Numeric columns are NOT
    NULL.
    """
    return [
        (
            name,                            # name
            get_type(value),                 # type_code
            None,                            # [display_size]
            None,                            # [internal_size]
            None,                            # [precision]
            None,                            # [scale]
            get_type(value) == Type.STRING,  # [null_ok]
        )
        for name, value in row.items()
    ]


def get_type(value):
    """Infer type from value."""
    if isinstance(value, string_types) or value is None:
        return Type.STRING
    elif isinstance(value, (int, float)):
        return Type.NUMBER
    elif isinstance(value, bool):
        return Type.BOOLEAN

    raise Error(f'Value of unknown type: {value}')


class DruidCursor(IteratorCursor):

    """Connection cursor."""

    def _set_results_and_description(self, query, *args, **kwargs):
        # `_stream_query` returns a generator that produces the rows; we need
        # to consume the first row so that `description` is properly set, so
        # let's consume it and insert it back.
        results = self._stream_query(query)
        try:
            first_row = next(results)
            self._results = itertools.chain([first_row], results)
        except StopIteration:
            self._results = iter([])

    def _stream_query(self, query):
        """
        Stream rows from a query.

        This method will yield rows as the data is returned in chunks from the
        server.
        """
        self.description = None

        headers = {'Content-Type': 'application/json'}
        payload = {'query': query}
        r = requests.post(self.url, stream=True, headers=headers, json=payload)
        if r.encoding is None:
            r.encoding = 'utf-8'

        # raise any error messages
        if r.status_code != 200:
            payload = r.json()
            msg = (
                f'{payload["error"]} ({payload["errorClass"]}): ' +
                f'{payload["errorMessage"]}'
            )
            raise ProgrammingError(msg)

        # Druid will stream the data in chunks of 8k bytes, splitting the JSON
        # between them; setting `chunk_size` to `None` makes it use the server
        # size
        chunks = r.iter_content(chunk_size=None, decode_unicode=True)
        Row = None
        for row in rows_from_chunks(chunks):
            # update description
            if self.description is None:
                self.description = get_description_from_row(row)

            # return row in namedtuple
            if Row is None:
                Row = namedtuple('Row', row.keys(), rename=True)
            yield Row(*row.values())


class DruidConnection(Connection):

    """Connection to a Druid database."""

    cursor_class = DruidCursor


def rows_from_chunks(chunks):
    """
    A generator that yields rows from JSON chunks.

    Druid will return the data in chunks, but they are not aligned with the
    JSON objects. This function will parse all complete rows inside each chunk,
    yielding them as soon as possible.
    """
    body = ''
    for chunk in chunks:
        if chunk:
            body = f'{body}{chunk}'

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
            yield row
