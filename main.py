"""Import Dict and Union for type-hinting"""
from typing import Dict, Union
import requests
import pandas as pd
import numpy as np

class APIDataExtractor:
    """Extracting players data from API league"""

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
        """Call make_api_request() and return the resulting data as a DataFrame"""
        data = self.make_api_request()
        df = pd.DataFrame(data['entries'])
        return df.to_csv('data.csv')


class DataTransformator:
    """Changing structure and types of data, preparing for SCD2"""

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
        # Assigning new names
        new_df = new_df.rename(columns={
            'summonerName': 'summonerName_x',
            'leaguePoints': 'leaguePoints_x',
            'wins': 'wins_x',
            'losses': 'losses_x',
            'win_loss_ratio': 'win_loss_ratio_x',
        })
        return new_df

    def create_dim_model(self) -> None:
        """Creating new columns(“DimKey”,”ValidFrom”,”ValidTo” and “IsCurrent”) and new .csv file"""
        dim_df = self.columns_change()
        dim_df = dim_df.rename(columns={
            'summonerName_x': 'summonerName_y',
            'leaguePoints_x': 'leaguePoints_y',
            'wins_x': 'wins_y',
            'losses_x': 'losses_y',
            'win_loss_ratio_x': 'win_loss_ratio_y'
        })
        dim_df = dim_df.assign(DimKey=dim_df.index)
        dim_df = dim_df.assign(ValidFrom=pd.to_datetime('today').strftime("%Y%m%d"))
        dim_df = dim_df.assign(ValidTo='99991231')
        dim_df = dim_df.assign(IsCurrent='1')
        return dim_df.to_csv('dim_data.csv', index=False)

    def implemeting_scd2(self) -> pd.DataFrame:
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
        # Identify SCD Type 2 records By comparing SCD2 fields in source and dimension
        df_scd2_records = df_excluding_new[
            (df_excluding_new["leaguePoints_x"] != df_excluding_new["leaguePoints_y"]) |
            (df_excluding_new["wins_x"] != df_excluding_new["wins_y"]) |
            (df_excluding_new["losses_x"] != df_excluding_new["losses_y"]) |
            (df_excluding_new["win_loss_ratio_x"] != df_excluding_new["win_loss_ratio_y"])
            ]
        # Join dataframe and exclude duplicates (remove scd2 records)
        df_excluding_new_scd2 = pd.concat([df_excluding_new, df_scd2_records]) \
            .drop_duplicates(keep=False)
        # Identify SCD Type 1 Records By comparing SCD1
        # fields in source and dimension
        df_scd1_records = df_excluding_new[(df_excluding_new["summonerName_x"]
                                            != df_excluding_new["summonerName_y"])]
        # Join dataframe and exclude duplicates
        # (remove scd1 records - no change records remaining)
        df_no_change_records = pd.concat([df_excluding_new_scd2, df_scd1_records]) \
            .drop_duplicates(keep=False)
        # Rename required No Change Fields
        df_no_change_rename = df_no_change_records.rename(columns={
            "leaguePoints_x": "leaguePoints", "wins_x": "wins", 'losses_x': 'losses',
            "win_loss_ratio_x": "win_loss_ratio",
            "summonerName_x": "summonerName"
        })

        # Select required No Change Fields fields
        df_no_change_final = df_no_change_rename[[
            'DimKey', 'summonerId', 'leaguePoints', 'wins', 'losses', 'win_loss_ratio', 'summonerName', 'ValidFrom',
            'ValidTo', 'IsCurrent'
        ]]
        # Rename required SCD1 Fields
        df_scd1_rename = df_scd1_records.rename(columns={
            "leaguePoints_x": "leaguePoints", "wins_x": "wins", 'losses_x': 'losses',
            "win_loss_ratio_x": "win_loss_ratio",
            "summonerName_x": "summonerName"
        })
        # Select required SCD1 Fields
        df_scd1_final = df_scd1_rename[[
            'DimKey', 'summonerId', 'leaguePoints', 'wins', 'losses', 'summonerName', 'win_loss_ratio', 'ValidFrom',
            'ValidTo', 'IsCurrent'
        ]]
        # Rename required SCD2 New Fields
        df_scd2New_rename = df_scd2_records.rename(columns={
            "leaguePoints_x": "leaguePoints", "wins_x": "wins", "losses_x": "losses",
            "win_loss_ratio_x": "win_loss_ratio",
            "summonerName_x": "summonerName"
        })
        # Update SCD2 New ValidFrom
        df_scd2_records["ValidFrom"] = pd.to_datetime('today').strftime("%Y%m%d")
        df_scd2_records["IsCurrrent"] = '1'
        # Select required SCD2 New Fields
        df_scd2new_final = df_scd2New_rename[[
            'DimKey', 'summonerId', 'leaguePoints', 'wins', 'losses', 'summonerName', 'win_loss_ratio', 'ValidFrom',
            'ValidTo', 'IsCurrent'
        ]]
        # Rename required SCD2 Old Fields
        df_scd2Old_rename = df_scd2_records.rename(columns={
            "leaguePoints_y": "leaguePoints", "wins_y": "wins", "losses_y": "losses",
            "win_loss_ratio_y": "win_loss_ratio",
            "summonerName_y": "summonerName"
        })
        # Update SCD2 Old ValidTo and IsCurrent
        TodaysDate = pd.Timestamp('today')
        df_scd2Old_rename["ValidTo"] = pd.to_datetime('today').strftime("%Y%m%d")
        df_scd2Old_rename["IsCurrent"] = 0
        # Select required SCD2 Old Fields
        df_scd2old_final = df_scd2Old_rename[
            ['DimKey', 'summonerId', 'leaguePoints', 'wins', 'losses', 'summonerName', 'win_loss_ratio', 'ValidFrom',
             'ValidTo', 'IsCurrent']]
        # Rename required New record Fields
        df_New_rename = df_new_records.rename(columns={
            "leaguePoints_x": "leaguePoints", "wins_x": "wins", "losses_x": "losses",
            "win_loss_ratio_x": "win_loss_ratio",
            "summonerName_x": "summonerName"
        })
        # Update New records ValidFrom
        df_New_rename["ValidFrom"] = 19000101
        df_New_rename["ValidTo"] = 99991231
        df_New_rename["IsCurrent"] = 1

        # Select required New record Fields
        df_new_final = df_New_rename[
            ['DimKey', 'summonerId', 'leaguePoints', 'wins', 'losses', 'summonerName', 'win_loss_ratio', 'ValidFrom',
             'ValidTo', 'IsCurrent']]

        # Union scd2 new and new records set DimKey
        df_new_new_scd2 = df_allframes = [df_scd2new_final, df_new_final]
        df_new_new_scd2_concat = pd.concat(df_new_new_scd2)
        df_new_new_scd2_concat['DimKey'] = np.arange(len(df_new_new_scd2_concat)) + 1 + max_dim_key

        # Union All Dataframes
        df_allframes = [df_scd2old_final, df_scd1_final, df_no_change_final, df_new_new_scd2_concat]
        df_allframes_concat = pd.concat(df_allframes)

        # Sort the data
        df_all_sort = df_allframes_concat.sort_values(["summonerId", "ValidFrom", "ValidTo"],
                                                      ascending=(True, True, True))

        return df_all_sort.to_string()


if __name__ == '__main__':
    URL = "https://euw1.api.riotgames.com/lol/league/v4/" \
          "challengerleagues/by-queue/RANKED_SOLO_5x5?api_key="
    API_KEY = ""
    FILE_PATH = "data.csv"
    DIM_FILE_PATH = "dim_data.csv"
    data_extractor = APIDataExtractor(URL, API_KEY)
    data_extractor2 = DataTransformator(FILE_PATH, DIM_FILE_PATH)
    print(data_extractor2.implemeting_scd2())
