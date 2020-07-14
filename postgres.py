import psycopg2

class PGDatabase:
    _SCHEMA = """
       CREATE TABLE IF NOT EXISTS peers (date date, peer varchar(64), count int);
    """

    def __init__(self, name, user, password=None, host='localhost', port=5432):
        self.db = psycopg2.connect(
            user = user,
            password = password,
            host = host,
            port = port,
            database = name
        )
        self.c = self.db.cursor()
        self._create_schema()

    def _create_schema(self):
        self.c.execute(self._SCHEMA)
        self.db.commit()

    def get_most_recent_day(self):
        rval = self.c.execute('SELECT date FROM peers ORDER BY date LIMIT 1;')
        return rval
