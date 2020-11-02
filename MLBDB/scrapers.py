import sys
import time
import json
import traceback

from selenium import webdriver
import pandas as pd
from pybaseball import statcast

from data import DataHandler

########################################################################################################################
## GAME INFO
########################################################################################################################
class GameInfoScraper():
    def __init__(self):
        """ A class to scrape info from the www.mlb.com/gameday website. Data scraped are attendance, duration, first_pitch_time, forecast, temp, ump_HP, ump_1B, ump_2B, ump_3B, venue, wind_direction, and wind_speed. NOTE: The website is not static, so selenium is used to render the webpage before scraping. However, this takes about 10 seconds per scrape so this is very slow. """
        self.driver = webdriver.Chrome(executable_path="venv/lib/chromedriver_86.0.4240.22")
        self.abbr_to_name = {
            "ATL" : {"city":"Atlanta", "name":"Braves"},
            "ARI" : {"city":"Arizona", "name":"D-backs"},
            "BAL" : {"city":"Baltimore", "name":"Orioles"},
            "BOS" : {"city":"Boston", "name":"Red-Sox"},
            "CHC" : {"city":"Chicago", "name":"Cubs"},
            "CIN" : {"city":"Cincinnati", "name":"Reds"},
            "CLE" : {"city":"Cleveland", "name":"Indians"},
            "COL" : {"city":"Colorado", "name":"Rockies"},
            "CWS" : {"city":"Chicago", "name":"White-Sox"},
            "DET" : {"city":"Detroit", "name":"Tigers"},
            "HOU" : {"city":"Houston", "name":"Astros"},
            "KC" : {"city":"Kansas City", "name":"Royals"},
            "LAA" : {"city":"Los Angeles", "name":"Angels"},
            "LAD" : {"city":"Los Angeles", "name":"Dodgers"},
            "MIA" : {"city":"Miami", "name":"Marlins"},
            "MIL" : {"city":"Milwaukee", "name":"Brewers"},
            "MIN" : {"city":"Minnesota", "name":"Twins"},
            "NYM" : {"city":"New York", "name":"Mets"},
            "NYY" : {"city":"New York", "name":"Yankees"},
            "OAK" : {"city":"Oakland", "name":"Athletics"},
            "PHI" : {"city":"Philadelphia", "name":"Phillies"},
            "PIT" : {"city":"Pittsburgh", "name":"Pirates"},
            "SD" : {"city":"San Diego", "name":"Padres"},
            "SEA" : {"city":"Seattle", "name":"Mariners"},
            "SF" : {"city":"San Francisco", "name":"Giants"},
            "STL" : {"city":"St. Louis", "name":"Cardinals"},
            "TB" : {"city":"Tampa Bay", "name":"Rays"},
            "TEX" : {"city":"Texas", "name":"Rangers"},
            "TOR" : {"city":"Toronto", "name":"Blue-Jays"},
            "WSH" : {"city":"Washington", "name":"Nationals"},
        }

    def _close_browser(self):
        """ Closes the selenium driver 
        
        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.driver.close()

    def _parse_attendance(self, att):
        """ Parses the attendance string from the game info.

        Parameters
        ----------
        att (str) : the string containing the attendance info

        Returns
        -------
        dict : a dictionary with the key attendance and the value as the attendance
        """
        return {"attendance" : int(att[1:-1].replace(',', ''))}

    def _parse_duration(self, duration):
        """ Parses the duration string from the game info.
        
        Parameters
        duration (str) : the string containing the duration info
        
        Returns
        -------
        dict : a dictionary with the key duraction and the value as the duration in minutes
        """
        dur_split = duration[1:-1].split(':')
        hours = dur_split[0]
        mins = dur_split[1][:2]
        return {"duration":60*int(hours)+int(mins)}

    def _parse_first_pitch(self, first_pitch):
        """ Parses the first pitch game time string from the game info.

        Parameters
        ----------
        first_pitch (str) : the string containing the first pitch info

        Returns
        -------
        dict : a dictionary with the key first_pitch_time and the value as the time
        """
        return {'first_pitch_time': first_pitch[1:-1]}

    def _parse_info(self, info):
        """ Parses the game info into a dictionary. If a specific info is not found it is not added to the dictionary, but everything else is.

        Parameters
        ----------
        info (str) : the info obtained by scraping the game

        Returns
        -------
        dict : a dictionary of all of the relevant information
        """
        lines = info.split("\n")
        att, duration, first_pitch, umps, venue, weather, wind = {}, {}, {}, {}, {}, {}, {}
        for line in lines:
            if line[0:4] == "Att:":
                att = self._parse_attendance(line[4:])
            elif line[0:12] == "First pitch:":
                first_pitch = self._parse_first_pitch(line[12:])
            elif line[0:2] == "T:":
                duration = self._parse_duration(line[2:])
            elif line[0:8] == "Umpires:":
                umps = self._parse_umpires(line[8:])
            elif line[0:6] == "Venue:":
                venue = self._parse_venue(line[6:])
            elif line[0:8] == "Weather:":
                weather = self._parse_weather(line[8:])
            elif line[0:5] == "Wind:":
                wind = self._parse_wind(line[5:])
        # game_info = att | duration | first_pitch | umps | venue | weather | wind
        game_info = {**att, **duration, **first_pitch, **umps, **venue, **weather, **wind}
        return game_info

    def _parse_umpires(self, umpires):
        """ Parses the umpire string from the game info into a dictionary where the keys are the umpire positions and the values are the names of the umpires 
        
        Parameters
        ----------
        umpires (str) : the string of umpire positions and names
        
        Returns
        -------
        dict : the dictionary mapping umpire positions to names
        """
        umps = umpires.split(":")
        final_umps = {}
        for i in range(len(umps) - 1):
            pos = umps[i][-2:]
            name = umps[i+1][1:-4] if i < len(umps) - 2 else umps[i+1][1:-1]
            final_umps[f"ump_{pos}"] = name
        return final_umps

    def _parse_venue(self, venue):
        """ Parses the venue string from the game info.

        Parameters
        ----------
        venue (str) : the string containing the venue info

        Returns
        -------
        dict : a dictionary with the key venue and the value as the venue name
        """
        return {"venue": venue[1:-1]}

    def _parse_weather(self, weather):
        """ Parses the weather string from the game info into a dictionary with the keys temp and forecast. Temp is the game's starting temperature in degrees fahrenheit and forecast is the conditions (sunny, partly_cloudy, rain, etc.)
        
        Parameters
        ----------
        weather (str) : the string of the weather information

        Returns
        -------
        dict : the dictionary of temperature and forecast
        """
        temp, forecast = weather.split(',')
        temp = int(temp.split(' ')[1])
        forecast = forecast[1:-1].replace(' ', '_').lower()
        return {"temp":temp, "forecast":forecast}

    def _parse_wind(self, wind):
        """ Parses the wind string from the game info into a dictionary with the keys wind_speed and wind_direction. The wind speed is in miles per hour. Both values are measured at the start of the game.
        
        Parameters
        ----------
        wind (str) : the string of wind information

        Returns
        -------
        dict : the dictionary of wind speed and direction
        """
        speed, direction = wind.split(',')
        speed = int(speed.split(' ')[1])
        direction = direction[1:-1].replace(' ', '_').lower()
        return {'wind_speed':speed, 'wind_direction':direction}

    def scrape_game_info(self, game_id, game_date, away_team, home_team):
        """ Scrapes data for the given game. The scraped data is intended to supplement statcast data, so duplciate information is not collected.
        
        Parameters
        ----------
        game_id (int or str) : the mlb assigned game_id. These are the same ids used in statcast
        
        game_date (str) : the date of the game in yyyy-mm-dd format.
        
        away_team (str) : the abbreviated name of the away team

        home_team (str) : the abbreviated name of the home team

        Returns
        -------
        dict : a dictionary of the relevant information. The keys are attendance, duration, first_pitch_time, forecast, temp, ump_HP, ump_1B, ump_2B, ump_3B, venue, wind_direction, and wind_speed.
        """
        away_name = self.abbr_to_name[away_team]['name'].lower()
        home_name = self.abbr_to_name[home_team]['name'].lower()
        date = game_date.replace('-', '/')
        url = f"https://www.mlb.com/gameday/{away_name}-vs-{home_name}/{date}/{game_id}#game_state=final,lock_state=final,game_tab=box,game={game_id}"
        self.driver.get(url)
        time.sleep(3)
        info = ""
        for i in range(20):
            try:
                elements = self.driver.find_elements_by_class_name("gd-primary-regular")
                info = elements[-1].text
            except:
                time.sleep(1)
                if i == 19:
                    print(f"Could not scrape for game {game_id} on {game_date} {away_team} @ {home_team}")
        return self._parse_info(info)

    def scrape_season(self, season):
        """ Gets the game info for every game in the given season. Saves in the file game_info_data/{season}.json. NOTE: The statcast download for the season must be in statcast_data/season.csv. This provides the game ids.

        Parameters
        ----------
        season (int) : the season to scrape

        Returns
        -------
        None
        """
        ids = pd.read_csv(f"statcast_data/{season}.csv").astype({'game_pk':int})
        ids = ids.loc[:, ['game_pk', 'game_date', 'home_team', 'away_team']]
        ids = ids.drop_duplicates().reset_index(drop=True)

        with open(f"game_info_data/{season}.json", 'r') as f:
            game_info = json.load(f)

        start = time.perf_counter()
        for _, row in ids.iterrows():
            try:
                game_id, date, home_team, away_team = row
                if str(game_id) not in game_info:
                    print(f"{game_id}: {away_team} @ {home_team} on {date}")
                    game_info[game_id] = self.scrape_game_info(game_id, date, away_team, home_team)
            except:
                tb = ''.join(traceback.format_tb(sys.exc_info()[2]))
                print(f"Unhandled Error: {sys.exc_info()[0]}\nTraceback:\n{tb}")
                break
        with open(f"game_info_data/{season}.json", 'w') as f:
            json.dump(game_info, f)
        self._close_browser()
        end = time.perf_counter()
        print(f"{end - start} seconds")

########################################################################################################################
## PLAYER INFO
########################################################################################################################
class PlayerInfoScraper():
    def __init__(self):
        """ A class to scrape info from the www.mlb.com/player website. Data scraped for each player are up to birth_day, birth_month, birth_year, college, debut_day, debut_month, debut_year, draft_pick, draft_round, draft_team, draft_year, full_name, handedness_batting, handedness_throwing, height, high_school, name, nickname, position. NOTE: The website is not static, so selenium is used to render the webpage before scraping. However, this takes about 10 seconds per scrape so this is very slow. """
        self.driver = webdriver.Chrome(executable_path="venv/lib/chromedriver_86.0.4240.22")

    def _close_browser(self):
        """ Closes the selenium driver 
        
        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.driver.close()

    def _parse_draft_info(self, draft_info):
        """ Parses draft year, team, round, and pick from the given info

        Parameters
        ----------
        draft_info (str) : the information about the draft

        Returns
        -------
        int, str, int, int : the draft year, team, round, pick

        """
        draft_info = draft_info.split(", ")

        return int(draft_info[0][1:]), draft_info[1], draft_info[2][7:], int(draft_info[3][14:])

    def _parse_height_weight(self, hw):
        """ Parses the height and weight from the given string 
        
        Parameters
        ----------
        hw (str) : the height and weight in the form 5' 9"/180

        Returns
        -------
        int : the height in inches

        int : the weight in pounds
        """
        height, weight = hw.split("/")
        height_ft, height_in = height.split("'")
        return int(height_ft)*12 + int(height_in[:-1]), int(weight)

    def _parse_info(self, player_vitals, player_bio):
        """ Parses the information from the vitals and bio sections of their mlb webpage.

        Parameters
        ----------
        player_vitals (str) : info from the vitals section

        player_bio (str) : info from the bio section

        Returns
        -------
        dict : the information parsed into a dictionary with the potential keys birth_day, birth_month, birth_year, college, debut_day, debut_month, debut_year, draft_pick, draft_round, draft_team, draft_year, full_name, handedness_batting, handedness_throwing, height, high_school, name, nickname, position
        """
        info = {}
        player_vitals = player_vitals.split("\n")
        player_bio = player_bio.split("\n")
        info["name"] = player_vitals[0].split('#')[0][:-1]
        info["position"] = player_vitals[1]
        info["handedness_batting"], info["handedness_throwing"] = player_vitals[2].split(' ')[1].split('/')
        info["height"], info["weight"] = self._parse_height_weight(player_vitals[3])
        info["full_name"] = player_bio[0]
        for line in player_bio[1:]:
            if line[:5] == "Born:":
                info["birth_month"], info["birth_day"], info["birth_year"] = [int(a) for a in line[5:].split(" in")[0].split("/")]
            elif line[:6] == "Debut:":
                info["debut_month"], info["debut_day"], info["debut_year"] = [int(a) for a in line[6:].split('/')]
            elif line[:6] == "Draft:":
                info["draft_year"], info["draft_team"], info["draft_round"], info["draft_pick"] = self._parse_draft_info(line[6:])
        return info

    def scrape_player(self, player_id):
        """ Scrapes an individual player's info from the mlb.com website 
        
        Parameters
        ----------
        player_id (int or str) : the id of the player to scrape

        Returns
        -------
        dict : the information about the player saved as a dictionary
        """
        url = f"https://www.mlb.com/player/{player_id}"
        self.driver.get(url)
        time.sleep(3)
        player_vitals, player_bio = "", ""
        for i in range(20):
            try:
                player_vitals = self.driver.find_elements_by_class_name("player-header--vitals")[0].text
                player_bio = self.driver.find_elements_by_class_name("player-bio")[0].text
            except:
                time.sleep(1)
                if i == 19:
                    print(f"Could not scrape for player {player_id}")
        return self._parse_info(player_vitals, player_bio)

    def scrape_all_players(self, year_start, year_end):
        """ Scrapes all of the players in the given year range. The info is saved in player_info_data/player_info.json. NOTE: the player ids come from the statcast data, so this must be scraped first 
        
        Parameters
        ----------
        year_start (int or str) : the first year to get player ids from
        
        year_end (int or str) : the last year to get player ids from

        Returns
        -------
        None
        """
        player_ids = []
        for year in range(year_start, year_end+1):
            data = pd.read_csv(f"statcast_data/{year}.csv").astype({'batter':int, 'pitcher':int})
            player_ids.append(data.loc[:, 'batter'].drop_duplicates())
            player_ids.append(data.loc[:, 'pitcher'].drop_duplicates())
        player_ids = pd.concat(player_ids)
        player_ids = player_ids.drop_duplicates().reset_index(drop=True)

        with open(f"player_info_data/player_info.json", 'r') as f:
            scraped_players = json.load(f)

        start = time.perf_counter()
        total_players = player_ids.shape[0]
        
        for idx, id in player_ids.iteritems():
            if idx%100 == 0:
                print(f"{idx}/{total_players}")
            try:
                if str(id) not in scraped_players:
                    print(id)
                    scraped_players[id] = self.scrape_player(id)
            except:
                tb = ''.join(traceback.format_tb(sys.exc_info()[2]))
                print(f"Unhandled Error: {sys.exc_info()[0]}\nTraceback:\n{tb}")
                break
        with open(f"player_info_data/player_info.json", 'w') as f:
            json.dump(scraped_players, f)
        
        self._close_browser()
        end = time.perf_counter()
        print(f"{end - start} seconds")

########################################################################################################################
## STATCAST
########################################################################################################################
class StatcastScraper():
    def __init__(self):
        """ A class to download the statcast data for a given season. Uses the pybaseball library."""
        self.season_dates = {
            2017 : {
                "start" : "2017-04-02",
                "end" : "2017-10-01"
            },
            2018 : {
                "start" : "2018-03-29",
                "end" : "2018-10-01",
            },
            2019 : {
                "start" : "2019-03-20",
                "end" : "2019-09-29"
            },
            2020 : {
                "start" : "2020-07-23",
                "end" : "2020-09-27",
            },
        }

    def _create_uid(self, row):
        """ Combines the date, game_pk, at_bat_number, and pitch_number from the given row to create a uid for the pitch event. The uid is sortable chronologically

        Parameters
        ----------
        row (DataFrame) : the row to create the id for

        Returns
        -------
        int : the unique id
        """
        month = f"{row['game_date'].month}".zfill(2)
        day = f"{row['game_date'].day}".zfill(2)
        game_id = int(row['game_pk'])
        ab_id = f"{int(row['at_bat_number'])}".zfill(3)
        pitch_id = f"{int(row['pitch_number'])}".zfill(2)
        return int(f"{row['game_date'].year}{month}{day}{game_id}{ab_id}{pitch_id}")

    def download_season(self, years):
        """ Downloads the statcast data for a season as a csv. It adds a unique id (uid) for each pitch and then saves the csv in the folder statcast_data/[year].csv.
        
        Parameters
        ----------
        years (str, int, list of strings, list of ints) : the season(s) to download

        Returns
        -------
        None
        """
        dh = DataHandler()
        if not isinstance(years, list):
            years = [years]
        for year in years:
            data = statcast(start_dt=self.season_dates[year]["start"], end_dt=self.season_dates[year]["end"])
            data["index"] = data.apply(lambda row : self._create_uid(row), axis=1)
            data = data.set_index("index")
            data = data.sort_index().reset_index(drop=False)
            data = dh.set_data_types(data)
            data.to_feather(f"statcast_data/{year}")


########################################################################################################################
## MAIN
########################################################################################################################
if __name__ == "__main__":
    statcast_scraper = StatcastScraper()
    statcast_scraper.download_season([2017, 2018, 2019, 2020])