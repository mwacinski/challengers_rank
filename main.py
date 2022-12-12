# Import Dict for type-hinting
from typing import Dict, Union
import requests
import pandas as pd

# Extracting players data from API league
class APIDataExtractor:
    def __init__(self, url: str, api_key: str):
        self.url = url
        self.api_key = api_key


    def make_api_request(self) -> Union[Dict, str]:
        """Getting informations from API request and if API key is wrong or expired
            an exception is given."""
        try:
            response = requests.get(f"{self.url}{self.api_key}", timeout=3)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as error:
                return error

    def load_data_to_csv(self) -> None:
        # Call make_api_request() and return the resulting data as a DataFrame
        data = self.make_api_request()
        df = pd.DataFrame(data['entries'])
        return df.to_csv('data.csv')


class DataTransformator:
    def __init__(self, file_path: str, dim_file_path: str):
        self.file_path = file_path
        self.dim_file_path = dim_file_path

    def columns_change(self) -> pd.DataFrame:
        """Creating new dataframe with appropriate data (Selecting few columns from old)"""
        df = pd.read_csv(self.file_path)
        new_df = df[['summonerId', 'summonerName', 'leaguePoints', 'wins', 'losses']].copy()
        # Fix Datatypes
        new_df['summonerId'] = new_df['summonerId'].astype(pd.StringDtype())
        new_df['summonerName'] = new_df['summonerName'].astype(pd.StringDtype())
        new_df['leaguePoints'] = new_df['leaguePoints'].astype(pd.Int64Dtype())
        new_df['wins'] = new_df['wins'].astype(pd.Int64Dtype())
        new_df['losses'] = new_df['losses'].astype(pd.Int64Dtype())
        # Creating win/loss ratio, rounded to 2 decimal places
        new_df = new_df.assign(win_loss_ratio=
                               round(new_df['wins'] / (new_df['wins'] + new_df['losses']) * 100, 2))
        new_df['win_loss_ratio'] = new_df['win_loss_ratio'].astype(pd.Float64Dtype())
        return new_df


    def create_dim_model(self) -> None:
        """Creating new columns(“DimKey”,”ValidFrom”,”ValidTo” and “IsCurrent”) and new .csv file"""
        dim_df = self.columns_change()
        dim_df = dim_df.rename(columns={
            'summonerName': 'summonerName_x',
            'leaguePoints': 'leaguePoints_x',
            'wins': 'wins_x',
            'losses': 'losses_x',
            'win_loss_ratio': 'win_loss_ratio_x',
        })
        dim_df = dim_df.assign(DimKey=dim_df.index)
        dim_df = dim_df.assign(ValidFrom=pd.to_datetime('today').strftime("%Y%m%d"))
        dim_df = dim_df.assign(ValidTo='99991231')
        dim_df = dim_df.assign(IsCurrent='1')
        return dim_df.to_csv('dim_data.csv', index=False)

    def implemeting_scd2(self) -> None:
        """Implemeting slowly changing dimensions type 2"""
        df = self.columns_change()
        dim_df = pd.read_csv(self.dim_file_path)
        # Get Max DimKey
        max_dim_key = dim_df['DimKey'].max()
        # Filter only Currnet records from Dimension
        df_dim_is_current = dim_df[(dim_df["IsCurrent"] == 1)]
        # Left Join dataframes on keyfields
        df_merge_col = pd.merge(df, dim_df, on='summonerId', how='left')
        # Fix Datatypes
        df_merge_col['DimKey'] = df_merge_col['DimKey'].astype(pd.Int64Dtype())
        df_merge_col['ValidFrom'] = df_merge_col['ValidFrom'].astype(pd.Int64Dtype())
        df_merge_col['ValidTo'] = df_merge_col['ValidTo'].astype(pd.Int64Dtype())
        df_merge_col['IsCurrent'] = df_merge_col['IsCurrent'].astype(pd.Int16Dtype())
        # Identify new records By checking if DimKey IsNull
        new_records_filter = pd.isnull(df_merge_col["DimKey"])
        # Create dataframe for new records
        df_new_records = df_merge_col[new_records_filter]
        # Join dataframe and exclude duplicates (remove new records)
        df_excluding_new = pd.concat([df_merge_col, df_new_records]).drop_duplicates(keep=False)
        ##Identify SCD Type 2 records By comparing SCD2 fields in source and dimension
        df_scd2_records = df_excluding_new[
            (df_excluding_new["leaguePoints"] != df_excluding_new["leaguePoints_x"]) |
            (df_excluding_new["wins"] != df_excluding_new["wins_x"]) |
            (df_excluding_new["losses"] != df_excluding_new["losses_x"]) |
            (df_excluding_new["win_loss_ratio"] != df_excluding_new["win_loss_ratio_x"])
        ]
        # Join dataframe and exclude duplicates (remove scd2 records)
        df_excluding_new_scd2 = pd.concat([df_excluding_new, df_scd2_records])\
            .drop_duplicates(keep=False)
        # Identify SCD Type 1 Records By comparing SCD1
        # fields in source and dimension
        df_scd1_records = df_excluding_new[(df_excluding_new["summonerName"]
                                            != df_excluding_new["summonerName_x"])]
        # Join dataframe and exclude duplicates
        # (remove scd1 records - no change records remaining)
        df_no_change_records = pd.concat([df_excluding_new_scd2, df_scd1_records])\
            .drop_duplicates(keep=False)
        # Update SCD2 New ValidFrom
        df_scd2_records["ValidFrom"] = pd.to_datetime('today').strftime("%Y%m%d")
        df_scd2_records["IsCurrrent"] = '1'

        return df_no_change_records



if __name__ == '__main__':
    URL = "https://euw1.api.riotgames.com/lol/league/v4/" \
    "challengerleagues/by-queue/RANKED_SOLO_5x5?api_key="
    API_KEY = "RGAPI-9ff647af-03fe-4a29-999c-8c84bf2db854"
    FILE_PATH = "data.csv"
    DIM_FILE_PATH = "dim_data.csv"

    data_extractor = DataTransformator(FILE_PATH, DIM_FILE_PATH)
    print(data_extractor.implemeting_scd2())
