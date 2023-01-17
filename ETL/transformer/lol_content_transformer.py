from datetime import datetime
import pandas as pd
import numpy as np


class LOLContentTransformer:
    """Changing structure and types of data, preparing for SCD2"""

    def __init__(self, path: str):
        self.path = path

    def columns_change(self) -> pd.DataFrame:
        """Creating new dataframe with appropriate data (Selecting few columns from old)"""
        df = pd.read_csv(f"{self.path}\\data.csv")
        new_df = df[['summonerId', 'summonerName', 'leaguePoints', 'wins', 'losses']].copy()

        # Fix Datatypes
        new_df['summonerId'] = new_df['summonerId'].astype(pd.StringDtype())
        new_df['summonerName'] = new_df['summonerName'].astype(pd.StringDtype())
        new_df['leaguePoints'] = new_df['leaguePoints'].astype(pd.Int64Dtype())
        new_df['wins'] = new_df['wins'].astype(pd.Int64Dtype())
        new_df['losses'] = new_df['losses'].astype(pd.Int64Dtype())

        # Creating win/loss ratio, rounded to 2 decimal places
        new_df = new_df.assign(win_loss_ratio=round(new_df['wins'] / (new_df['wins'] + new_df['losses']) * 100, 2))
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

    def create_dim_model(self) -> pd.DataFrame:
        """Creating new columns(DimKey, ValidFrom, ValidTo and IsCurrent) and new .csv file"""
        dim_df = self.columns_change()
        dim_df = dim_df.rename(columns={
            'summonerName_x': 'summonerName_y',
            'leaguePoints_x': 'leaguePoints_y',
            'wins_x': 'wins_y',
            'losses_x': 'losses_y',
            'win_loss_ratio_x': 'win_loss_ratio_y'
        })
        dim_df = dim_df.assign(DimKey=dim_df.index)
        dim_df = dim_df.assign(ValidFrom=datetime.now().strftime("%Y-%m-%d"))
        dim_df = dim_df.assign(ValidTo='2024-01-30')
        dim_df = dim_df.assign(IsCurrent='1')

        dim_df = dim_df[['DimKey', 'summonerId', 'leaguePoints_y', 'wins_y', 'losses_y', 'win_loss_ratio_y',
                         'summonerName_y', 'ValidFrom', 'ValidTo', 'IsCurrent']]
        return dim_df.to_csv(f"{self.path}\\dim_data.csv", index=False)

    def implementing_scd2(self) -> None:
        """Implementing slowly changing dimensions type 2
         https://blogs.sap.com/2021/10/06/sap-data-warehouse-cloud-how-to-create-a-slowly-changing-dimension/"""
        df = self.columns_change()
        dim_df = pd.read_csv(f"{self.path}\\dim_data.csv")

        # Get Max DimKey
        max_dim_key = dim_df['DimKey'].max()

        # Left Join dataframes on key fields
        df_merge_col = pd.merge(df, dim_df, on='summonerId', how='left')

        # Fix Datatypes
        df_merge_col['DimKey'] = df_merge_col['DimKey'].astype(pd.Int64Dtype())
        df_merge_col['ValidFrom'] = pd.to_datetime(df_merge_col['ValidFrom'], format='%Y-%m-%d')
        df_merge_col['ValidTo'] = pd.to_datetime(df_merge_col['ValidTo'], format='%Y-%m-%d')
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

        # Select required No Change Fields
        df_no_change_final = df_no_change_rename[[
            'DimKey', 'summonerId', 'leaguePoints', 'wins', 'losses', 'win_loss_ratio',
            'summonerName', 'ValidFrom', 'ValidTo', 'IsCurrent'
        ]]

        # Rename required SCD1 Fields
        df_scd1_rename = df_scd1_records.rename(columns={
            "leaguePoints_x": "leaguePoints", "wins_x": "wins", 'losses_x': 'losses',
            "win_loss_ratio_x": "win_loss_ratio", "summonerName_x": "summonerName"
        })

        # Select required SCD1 Fields
        df_scd1_final = df_scd1_rename[[
            'DimKey', 'summonerId', 'leaguePoints', 'wins', 'losses', 'summonerName',
            'win_loss_ratio', 'ValidFrom', 'ValidTo', 'IsCurrent'
        ]]

        # Rename required SCD2 New Fields
        df_scd2_new_rename = df_scd2_records.rename(columns={
            "leaguePoints_x": "leaguePoints", "wins_x": "wins", "losses_x": "losses",
            "win_loss_ratio_x": "win_loss_ratio", "summonerName_x": "summonerName"
        })

        # Update SCD2 New ValidFrom
        df_scd2_records["IsCurrent"] = '1'

        # Select required SCD2 New Fields
        df_scd2new_final = df_scd2_new_rename[[
            'DimKey', 'summonerId', 'leaguePoints', 'wins', 'losses', 'summonerName',
            'win_loss_ratio', 'ValidFrom', 'ValidTo', 'IsCurrent'
        ]]

        # Rename required SCD2 Old Fields
        df_scd2_old_rename = df_scd2_records.rename(columns={
            "leaguePoints_y": "leaguePoints", "wins_y": "wins", "losses_y": "losses",
            "win_loss_ratio_y": "win_loss_ratio", "summonerName_y": "summonerName"
        })

        # Update SCD2 Old ValidTo and IsCurrent
        df_scd2_old_rename["ValidTo"] = datetime.now().strftime("%Y-%m-%d")
        df_scd2_old_rename["IsCurrent"] = 0

        # Select required SCD2 Old Fields
        df_scd2_old_final = df_scd2_old_rename[
            ['DimKey', 'summonerId', 'leaguePoints', 'wins', 'losses', 'summonerName',
             'win_loss_ratio', 'ValidFrom', 'ValidTo', 'IsCurrent']]

        # Rename required New record Fields
        df_new_rename = df_new_records.rename(columns={
            "leaguePoints_x": "leaguePoints", "wins_x": "wins", "losses_x": "losses",
            "win_loss_ratio_x": "win_loss_ratio", "summonerName_x": "summonerName"
        })

        # Update New records ValidFrom
        df_new_rename["ValidFrom"] = datetime.now().strftime("%Y-%m-%d")
        df_new_rename["ValidTo"] = '2024-01-30'
        df_new_rename["IsCurrent"] = 1

        # Select required New record Fields
        df_new_final = df_new_rename[
            ['DimKey', 'summonerId', 'leaguePoints', 'wins', 'losses', 'summonerName',
             'win_loss_ratio', 'ValidFrom', 'ValidTo', 'IsCurrent']]

        # Union scd2 new and new records set DimKey
        df_new_new_scd2 = [df_scd2new_final, df_new_final]
        df_new_new_scd2_concat = pd.concat(df_new_new_scd2)
        df_new_new_scd2_concat['DimKey'] = np.arange(len(df_new_new_scd2_concat)) + 1 + max_dim_key

        # Union All Dataframes
        df_all_frames = [df_scd2_old_final, df_scd1_final, df_no_change_final, df_new_new_scd2_concat]
        df_all_frames_concat = pd.concat(df_all_frames)

        return df_all_frames_concat.to_csv(f"{self.path}\\final_data.csv", index=False)
