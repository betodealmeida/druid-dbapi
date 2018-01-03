# A Python DB API 2.0 for Druid #

This module allows accessing Druid via its [experimental SQL API](http://druid.io/docs/latest/querying/sql.html).

## Usage ##

Using the DB API:

```python
from druiddb import connect

conn = connect(host='localhost', port=8082, path='/druid/v2/sql/', scheme='http')
curs = conn.cursor()
curs.execute("""
    SELECT place,
           CAST(REGEXP_EXTRACT(place, '(.*),', 1) AS FLOAT) AS lat,
           CAST(REGEXP_EXTRACT(place, ',(.*)', 1) AS FLOAT) AS lon
      FROM places
     LIMIT 10
""")
for row in curs:
    print(row)
```
        
Using SQLAlchemy:

```python
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

engine = create_engine('druid://localhost:8082/druid/v2/sql/')  # uses HTTP by default :(
# engine = create_engine('druid+http://localhost:8082/druid/v2/sql/')
# engine = create_engine('druid+https://localhost:8082/druid/v2/sql/')

places = Table('places', MetaData(bind=engine), autoload=True)
print(select([func.count('*')], from_obj=places).scalar())
```

Using the REPL:

```bash
$ druiddb http://localhost:8082/druid/v2/sql/
> SELECT COUNT(*) AS cnt FROM places
  cnt
-----
12345
```
