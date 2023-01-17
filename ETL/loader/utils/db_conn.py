from psycopg2 import connect


class DbConnection:
    """A database connection"""

    def __init__(self, dbname, user, pwd):
        """Initialize the connection information"""

        self.dbname = dbname
        self.user = user
        self.pwd = pwd
        self.conn = None

    def connect(self):
        """Connect to the database"""

        try:
            self.conn = connect(dbname=self.dbname, user=self.user, password=self.pwd)
        except Exception as exc:
            print(exc)

    def close(self):
        """Close the database connection"""
        self.conn.close()
        self.conn = None

    def execute(self, query: str, args=None):
        """Create a cursor, execute a query and return the cursor"""

        self.connect()
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, args)
        except Exception as exc:
            cursor.close()
            raise exc
        return cursor

    def commit(self):
        """Commit currently open transaction"""

        self.conn.commit()

    def copy_from_csv(self, path, table_name):
        """Execute a COPY from a CSV file"""

        if self.conn is None or self.conn.closed:
            self.connect()
        with open(path, 'r') as f:
            cursor = self.conn.cursor()
            try:
                sql = f"COPY {table_name} FROM STDIN DELIMITER ',' CSV HEADER"
                cursor.copy_expert(sql, f)
            except Exception as exc:
                cursor.close()
                raise exc

    def copy_from_sql(self, sql):
        """Execute a COPY SQL command from a file"""

        if self.conn is None or self.conn.closed:
            self.connect()
        with open(sql, 'r') as f:
            cursor = self.conn.cursor()
            try:
                sql = f.read()
                self.execute(sql)
            except Exception as exc:
                cursor.close()
                raise exc
