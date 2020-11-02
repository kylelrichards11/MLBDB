from datetime import datetime

import dask.dataframe as dd
from dask.distributed import Client, progress
import numpy as np
import pandas as pd

from data import DataHandler

########################################################################################################################
## FEATURE MAKER
########################################################################################################################
class FeatureMaker():
    def __init__(self):
        self._dh = DataHandler()

    def _create_event_order_col(self, df, col_name):
        """ Creates a column that is the event order of the given DataFrame after sorting chronologically (by index)
        
        Parameters
        ----------
        df (DataFrame) : the data to be sorted

        col_name (str) : the name of the new column

        Returns
        -------
        DataFrame : the input DataFrame with the new column added
        """
        df = df.sort_index()
        df[col_name] = np.arange(0, df.shape[0])
        return df

    def _days_between(self, date_1, date_2, max_gap=None):
        """ Calculates the number of days between the given dates. If the days are sequential, then there are 0 days in between. If the max is not None, then any number of days greater than the max is returned as the max

        Parameters
        ----------
        date_1 (str) : date in the format yyyy-mm-dd

        date_2 (str) : date in the format yyyy-mm-dd

        Returns
        -------
        int : the number of days between the two dates
        """
        dt_1 = datetime.strptime(date_1, "%Y-%m-%d")
        dt_2 = datetime.strptime(date_2, "%Y-%m-%d")
        if max_gap is None:
            return max(0, abs((dt_1 - dt_2).days) - 1)
        return min(max_gap, max(0, abs((dt_1 - dt_2).days) - 1))

    def appearance_gap(self, data, player_type):
        """ Returns the data with a feature called appearance_gap. For each appearance for each player in the dataset, the number of days between the current appearance and the last appearance is calculated. Each gap is calculated for batting or pitching separately. Fielding appearances are currently not calculated. For example, if a player played on 07-05-2019 and then again on 07-07-2019, the gap is 1. This number is capped at 14, so any gaps longer than this will also be 14. This is because the difference between large gaps is insignificant. 
        
        Parameters
        ----------
        data (DataFrame) : the dataset to calculate the appearance gap for. There must be columns game_date, game_pk, pitcher, batter

        player_type (str) : "pitcher" or "batter", the type of player to calculate the pitch count for

        Returns
        -------
        DataFrame : the dataset with the feature appearance_gap
        """
        def calc_gap(df, player_type):
            df = df.drop_duplicates("game_date")
            df = df.sort_index()
            df["prev_date"] = df["game_date"].shift(fill_value="0001-01-01")
            df[f"appearance_gap_{player_type}"] = df.apply(lambda row : self._days_between(row["prev_date"], row["game_date"], max_gap=14), axis=1)
            df = df.drop(columns=["prev_date"])
            return df

        data_temp = data.loc[:, ["game_date", "game_pk", player_type]]
        meta = self._dh.get_data_types(df=data_temp, extra_cols=[f"appearance_gap_{player_type}"])
        gaps = data_temp.groupby(player_type).apply(calc_gap, player_type, meta=meta).reset_index(drop=True).drop(columns=["game_date"])
        return data.merge(gaps)

    def at_bat_count(self, data, player_type):
        """ Calculates the number of at bats the player has had during the game and adds it as a feature 
        
        Parameters
        ----------
        data (DataFrame) : the dataset to calculate the at_bat_count for. There must be columns game_pk, pitcher or batter, and at_bat_number

        player_type (str) : "pitcher" or "batter", the type of player to calculate the pitch count for

        Returns
        -------
        DataFrame : the dataset with the feature at_bat_count_pitcher or at_bat_count_batter
        """
        assert(player_type == "pitcher" or player_type == "batter")
        #TODO: Finish function
    
    def pitch_count(self, data, player_type):
        """ Calculates the number of previous pitches seen by each player for each pitch in the given outing and adds it as a feature

        Parameters
        ----------
        data (DataFrame) : the dataset to calculate the appearance gap for. There must be columns game_pk, pitcher or batter, at_bat_number, pitch_number

        player_type (str) : "pitcher" or "batter", the type of player to calculate the pitch count for

        Returns
        -------
        DataFrame : the dataset with the feature pitch_count_pitcher or pitch_count_batter
        """
        assert(player_type == "pitcher" or player_type == "batter")
    
        meta = self._dh.get_data_types(df=data, extra_cols=[f"pitch_count_{player_type}"])
        return data.groupby([player_type, "game_pk"]).apply(self._create_event_order_col, f"pitch_count_{player_type}", meta=meta)

if __name__ == "__main__":
    client = Client(n_workers=2, threads_per_worker=2, memory_limit='4GB')
    print(f"Dask dashboard at {client.dashboard_link}")

    dh = DataHandler()
    fm = FeatureMaker()

    data = dh.load_all_seasons()

    data = fm.pitch_count(data, "pitcher")
    # data = fm.pitch_count(data, "batter")
    # data = fm.appearance_gap(data, "pitcher")
    # data = fm.appearance_gap(data, "batter")

    data = data.compute()
