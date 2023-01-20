from dataclasses import dataclass
import os


class Config:

    URL = "https://euw1.api.riotgames.com/lol/league/v4/" \
          "challengerleagues/by-queue/RANKED_SOLO_5x5?api_keys="

    API_KEY = ""

    PROJECT_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

    DATA_DIR = os.path.join(PROJECT_DIR, "data")
    LOADER_UTILS_DIR = os.path.join(PROJECT_DIR, "ETL/loader/utils")


@dataclass
class DBCreds:
    DBNAME: str = "postgres"
    USER: str = "super_admin"
    PWD: str = "SomeSecretPassword"
