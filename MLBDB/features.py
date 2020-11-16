from datetime import datetime
import time

import dask.dataframe as dd
from dask.distributed import Client, progress
import numpy as np
import pandas as pd

from data import DataHandler
from utils import print_time

########################################################################################################################
## FEATURE MAKER
########################################################################################################################
class FeatureMaker():
    def __init__(self):
        self._dh = DataHandler()

    def _create_event_order_col(self, df, col_name, col_type):
        """ Creates a column that is the event order of the given DataFrame after sorting chronologically (by index)
        
        Parameters
        ----------
        df (DataFrame) : the data to be sorted

        col_name (str) : the name of the new column
        
        col_type (str) : the type of the new column

        Returns
        -------
        DataFrame : the input DataFrame with the new column added
        """
        df = df.sort_index()
        order = pd.Series(np.arange(0, df.shape[0]).astype('int8'), name=col_name)
        index = df["index"].reset_index(drop=True).astype('int')
        return pd.concat((index, order), axis=1)

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
        data = data.merge(gaps)
        return self._dh.set_index(data, "index")

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

        def count(df, col_name):
            df_temp = df.loc[:, ["index", "at_bat_number"]]
            df_temp = df_temp.sort_index()
            ab_nums = df_temp.drop_duplicates(subset=["at_bat_number"]).reset_index(drop=True)
            ab_counts = pd.concat((ab_nums, pd.Series(np.arange(0, ab_nums.shape[0]), name=col_name)), axis=1)
            data = df_temp.merge(ab_counts, left_on="at_bat_number", right_on="at_bat_number").drop(columns="index_y").rename(columns={"index_x":"index"})
            return data.loc[:, ["index", col_name]]

        assert(player_type == "pitcher" or player_type == "batter")
        meta = self._dh.get_data_types(extra_cols=["index", f"at_bat_count_{player_type}"])
        at_bat_counts = data.groupby([player_type, "game_pk"]).apply(count, f"at_bat_count_{player_type}", meta=meta)
        data = data.merge(at_bat_counts)
        return self._dh.set_index(data, "index")
    
    def pitch_count(self, data, player_type):
        """ Calculates the number of previous pitches seen by each player in each game and adds it as a feature

        Parameters
        ----------
        data (DataFrame) : the dataset to calculate the pitch count for. There must be columns game_pk, pitcher or batter

        player_type (str) : "pitcher" or "batter", the type of player to calculate the pitch count for

        Returns
        -------
        DataFrame : the dataset with the feature pitch_count_pitcher or pitch_count_batter
        """
        assert(player_type == "pitcher" or player_type == "batter")
        meta = self._dh.get_data_types(extra_cols=["index", f"pitch_count_{player_type}"])
        pitch_counts = data.groupby([player_type, "game_pk"]).apply(
            self._create_event_order_col, f"pitch_count_{player_type}", "Int8", meta=meta
        )
        data = data.merge(pitch_counts)
        return self._dh.set_index(data, "index")

if __name__ == "__main__":
    client = Client(n_workers=2, threads_per_worker=2, memory_limit='4GB')
    print(f"Dask dashboard at {client.dashboard_link}")

    dh = DataHandler()
    fm = FeatureMaker()

    columns = [
        "at_bat_number",
        "away_score",
        "away_team",
        "balls",
        "bat_score",
        "batter",
        "bb_type",
        "des",
        "description",
        "events",
        "fld_score",
        "game_date",
        "game_pk",
        "home_score",
        "home_team",
        "if_fielding_alignment",
        "index",
        "inning",
        "inning_topbot",
        "of_fielding_alignment",
        "on_1b",
        "on_2b",
        "on_3b",
        "outs_when_up",
        "p_throws",
        "pitch_name",
        "pitch_number",
        "pitch_type",
        "pitcher",
        "stand",
        "strikes",
        "type"
    ]

    # data = dh.load_all_seasons(columns=columns, npartitions=8)
    data = dh.load_season(2020, columns=columns, npartitions=8)
    print(data.shape)
    stime = time.perf_counter()
    data = fm.at_bat_count(data, "pitcher")
    data = fm.at_bat_count(data, "batter")
    data = fm.pitch_count(data, "pitcher")
    data = fm.pitch_count(data, "batter")
    data = fm.appearance_gap(data, "pitcher")
    data = fm.appearance_gap(data, "batter")
    data = data[data["pitcher"] == 453286]
    data = data.set_index("index")
    data = data.compute()
    etime = time.perf_counter()
    print_time(stime, etime)
    data.to_csv("temp.csv")