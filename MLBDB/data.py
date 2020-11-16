import dask
import dask.dataframe as dd
import feather

########################################################################################################################
## DATA Handler
########################################################################################################################
class DataHandler():
    def __init__(self):
        """ A class to handle basic data related tasks """
        self.data_types = {
            "appearance_gap_batter": "i4",
            "appearance_gap_pitcher": "i4",
            "at_bat_number": "i8",
            "at_bat_count_pitcher": "i8",
            "at_bat_count_batter": "i8",
            "away_score": "Int8",
            "away_team": "str",
            "ax": "float",
            "ay": "float",
            "az": "float",
            "babip_value": "Int8",
            "balls": "i4",
            "bat_score": "Int8",
            "batter": "Int32",
            "bb_type": "str",
            "break_angle_deprecated": "float",
            "break_length_deprecated": "float",
            "des": "str",
            "description": "str",
            "effective_speed": "float",
            "estimated_ba_using_speedangle": "float",
            "estimated_woba_using_speedangle": "float",
            "events": "str",
            "fielder_2": "Int32",
            "fielder_2.1": "Int32",
            "fielder_3": "Int32",
            "fielder_4": "Int32",
            "fielder_5": "Int32",
            "fielder_6": "Int32",
            "fielder_7": "Int32",
            "fielder_8": "Int32",
            "fielder_9": "Int32",
            "fld_score": "Int8",
            "game_date": "str",
            "game_pk": "Int32",
            "game_type": "str",
            "game_year": "Int16",
            "hc_x": "float",
            "hc_y": "float",
            "hit_distance_sc": "Int16",
            "hit_location": "Int8",
            "home_score": "Int8",
            "home_team": "str",
            "if_fielding_alignment": "str",
            "index": "int",
            "inning": "Int8",
            "inning_topbot": "str",
            "iso_value": "Int8",
            "launch_angle": "float",
            "launch_speed": "float",
            "launch_speed_angle": "Int8",
            "of_fielding_alignment": "str",
            "on_1b": "Int32",
            "on_2b": "Int32",
            "on_3b": "Int32",
            "outs_when_up": "i4",
            "p_throws": "str",
            "pfx_x": "float",
            "pfx_z": "float",
            "pitch_count_batter": "Int8",
            "pitch_count_pitcher": "Int8",
            "pitch_name": "str",
            "pitch_number": "Int8",
            "pitch_type": "str",
            "pitcher": "Int32",
            "pitcher.1": "Int32",
            "plate_x": "float",
            "plate_z": "float",
            "player_name": "str",
            "post_away_score": "Int8",
            "post_bat_score": "Int8",
            "post_fld_score": "Int8",
            "post_home_score": "Int8",
            "release_extension": "float",
            "release_pos_x": "float",
            "release_pos_y": "float",
            "release_pos_z": "float",
            "release_speed": "float",
            "release_spin_rate": "Int16",
            "spin_dir": "str",
            "spin_rate_deprecated": "float",
            "stand": "str",
            "strikes": "i4",
            "sv_id": "str",
            "sz_bot": "float",
            "sz_top": "float",
            "tfs_deprecated": "str",
            "tfs_zulu_deprecated": "str",
            "type": "str",
            "umpire": "str",
            "vx0": "float",
            "vy0": "float",
            "vz0": "float",
            "woba_denom": "Int8",
            "woba_value": "float",
            "zone": "Int8",
        }

    def get_data_types(self, df=None, extra_cols=[]):
        """ Gets the data types for the columns in the give DataFrame 
        
        Parameters
        ----------
        df (DataFrame), default=None : the DataFrame to get data types for

        extra_cols (list of str), default=[] : any columns not in the given dataframe whose types are needed

        Returns
        -------
        dict : a mapping of column names to data types
        """
        df_cols = [] if df is None else list(df.columns)
        return { key: self.data_types[key] for key in df_cols + extra_cols }

    def load_all_seasons(self, columns=None, npartitions=8):
        """ Loads all of the seasons in the statcast_data folder
        
        Parameters
        ----------
        columns (list, default=None) : columns to read. If None, all columns are read

        npartitions(int, default=8) : the number of pandas DataFrames to split the Dask DataFrame into

        Returns
        -------
        DataFrame : the data from the statcast_data folder as one DataFrame
        """
        datasets = [dask.delayed(feather.read_dataframe)(f"statcast_data/{year}", columns=columns) for year in range(2017, 2021)]
        meta = self.get_data_types(df=dd.from_delayed(datasets[0]))
        df = dd.from_delayed(datasets, meta=meta).repartition(npartitions=npartitions)
        df.set_index("index")
        return df

    def load_season(self, season, columns=None, npartitions=8):
        """ Loads a single season from the statcast_data folder
        
        Parameters
        ----------
        season (int, str) : the season to load

        columns (list, default=None) : columns to read. If None, all columns are read

        npartitions(int, default=8) : the number of pandas DataFrames to split the Dask DataFrame into

        Returns
        --------
        DataFrame : the data from the specified season
        """
        df = [dask.delayed(feather.read_dataframe)(f"statcast_data/{season}", columns=columns)]
        meta = self.get_data_types(df=dd.from_delayed(df))
        df = dd.from_delayed(df, meta=meta).repartition(npartitions=npartitions)
        df.set_index("index")
        return df

    def set_data_types(self, df):
        """ Sets the data types for the columns in the give DataFrame. If a column fails to set, each column is tried and the failing column name is printed and the column is saved to error_[column name].csv.
        
        Parameters
        ----------
        df (DataFrame) : the DataFrame to get data types for

        Returns
        -------
        dict : a mapping of column names to data types
        """
        dtypes = self.get_data_types(df=df)
        try:
            return df.astype(dtypes)
        except TypeError as e:
            print(e)
            for col in list(df.columns):
                try:
                    df.loc[:, col].astype(dtypes[col])
                except TypeError as e:
                    print(f"Error converting column {col}")
                    print(e)
                    temp = df.loc[:, col]
                    temp.to_csv(f'error_{col}.csv')
            exit()

    def set_index(self, df, index_col):
        """ Sets the index of the given DataFrame to the specified column. The column is not deleted and the index is named to pitch_id 
        
        Parameters
        ----------
        df (DataFrame) : the DataFrame to set the index for

        index_col (str) : the name of the column in the DataFrame to use as the new index

        Returns
        -------
        DataFrame : the input DataFrame with the index set
        """
        df = df.rename(columns={"index":"pitch_id"})
        df = df.set_index("pitch_id", drop=False)
        df = df.rename(columns={"pitch_id":"index"})
        return df