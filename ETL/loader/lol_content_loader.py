from ETL.loader.utils.db_conn import DbConnection
from config import DBCreds as DB


class LOLContentLoader:

    def __init__(self, path: str):
        self.path = path
        self.table_name = "challengers_rank"
        self.dbname = DB.POSTGRES_DATABASE
        self.user = DB.POSTGRES_USER
        self.password = DB.POSTGRES_PASSWORD
        self.host = DB.POSTGRES_HOST

    def load_content(self) -> None:
        db = DbConnection(self.dbname, self.user, self.password, self.host)
        try:
            db.copy_from_csv(f"{self.path}\\final_data.csv", self.table_name)
            db.commit()
        except Exception as err:
            print("Error while loading to Postgres", err)
        finally:
            if db:
                db.close()
                print("PostgresQL connection is closed")
