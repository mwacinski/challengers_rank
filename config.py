import os
from dataclasses import dataclass


class Config:

    API_KEY = ""
    URL = "https://euw1.api.riotgames.com/lol/league/v4/" \
          "challengerleagues/by-queue/RANKED_SOLO_5x5?api_key="

    PROJECT_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

    DATA_DIR = os.path.join(PROJECT_DIR, "data")
    LOADER_UTILS_DIR = os.path.join(PROJECT_DIR, "ETL\\loader\\utils")


@dataclass
class DBCreds:
    POSTGRES_DATABASE = "postgres"
    POSTGRES_USER = "admin"
    POSTGRES_PASSWORD = "admin"
    POSTGRES_HOST = "host.docker.internal"
