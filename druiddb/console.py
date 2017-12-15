from __future__ import unicode_literals

import os
import sys

from prompt_toolkit import prompt, AbortAction
from prompt_toolkit.history import FileHistory
from prompt_toolkit.contrib.completers import WordCompleter
from pygments.lexers import SqlLexer
from pygments.style import Style
from pygments.token import Token
from pygments.styles.default import DefaultStyle
from six.moves.urllib import parse
from tabulate import tabulate

from druiddb import connect


words = [
    'CREATE',
    'SELECT',
    'INSERT',
    'DROP',
    'DELETE',
    'FROM',
    'WHERE',
    'TABLE',
]
sql_completer = WordCompleter(words, ignore_case=True)


class DocumentStyle(Style):
    styles = {
        Token.Menu.Completions.Completion.Current: 'bg:#00aaaa #000000',
        Token.Menu.Completions.Completion: 'bg:#008888 #ffffff',
        Token.Menu.Completions.ProgressButton: 'bg:#003333',
        Token.Menu.Completions.ProgressBar: 'bg:#00aaaa',
    }
    styles.update(DefaultStyle.styles)


def main():
    history = FileHistory(os.path.expanduser('~/.druiddb.txt'))

    try:
        url = sys.argv[1]
    except IndexError:
        url = 'http://localhost:8082/druid/v2/sql/'

    parts = parse.urlparse(url)
    if ':' in parts.netloc:
        host, port = parts.netloc.split(':', 1)
        port = int(port)
    else:
        host = parts.netloc
        port = 8082

    connection = connect(host=host, port=port,
                         path=parts.path, scheme=parts.scheme)

    while True:
        try:
            query = prompt(
                '> ', lexer=SqlLexer, completer=sql_completer,
                style=DocumentStyle, history=history,
                on_abort=AbortAction.RETRY)
        except EOFError:
            break  # Control-D pressed.
        with connection as cursor:
            result = cursor.execute(query.rstrip(';'))
            headers = [t[0] for t in cursor.description]
            print(tabulate(result, headers=headers))

    print('GoodBye!')


if __name__ == '__main__':
    main()
