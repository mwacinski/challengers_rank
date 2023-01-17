from ETL.loader.utils.db_conn import DbConnection
from ETL.config import DBCreds as DBC


class LOLContentLoader:

    def __init__(self, content: str, path: str):
        self.content = content
        self.path = path
        self.table_name = "challengers_rank"

    def load_content(self) -> None:
        db = DbConnection(DBC.DBNAME, DBC.USER, DBC.PWD)
        try:
            db.copy_from_sql(self.content)
            db.commit()
        except Exception as err:
            print("Error while creating table in Postgres", err)
        try:
            db.copy_from_csv(self.path, self.table_name)
            db.commit()
        except Exception as err:
            print("Error while loading to Postgres", err)
        finally:
            if db:
                db.close()
                print("PostgresQL connection is closed")
