from typing import Dict, Union
import requests
import pandas as pd
from requests.exceptions import (
    ConnectionError,
    HTTPError
)


class LOLContentExtractor:
    """Extracting players data from API league"""

    def __init__(self, url: str, api_key: str, path: str):
        self.url = url
        self.api_key = api_key
        self.path = path

    def make_api_request(self) -> Union[Dict, requests.exceptions.HTTPError]:
        """Getting information from API request and if API key is wrong or expired
            an exception is given."""
        try:
            response = requests.get(f"{self.url}{self.api_key}", timeout=3)
            response.raise_for_status()
            return response.json()

        except ConnectionError as con_err:
            print(f"Connection error occurred. More info: {con_err}")

        except HTTPError as http_err:
            print(f"HTTP error occurred. More info: {http_err}")

    def extract_data(self) -> None:
        """Call make_api_request() and return the resulting data as a DataFrame"""
        data = self.make_api_request()
        df = pd.DataFrame(data['entries'])
        return df.to_csv(f"{self.path}\\data.csv")
