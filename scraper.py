from pandas.io.json import json_normalize
import pandas as pd
import requests

class WNBAScraper:

    headers = {'user-agent': ('Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'),
               'Dnt': ('1'),
               'Accept-Encoding': ('gzip, deflate, sdch'),
               'Accept-Language': ('en'),
               'origin': ('http://stats.nba.com')}
    id_prefix = 'http://data.wnba.com/data/5s/v2015/json/mobile_teams/wnba/'
    log_prefix = 'http://stats.nba.com/stats/playergamelog?LeagueID=10&playerid='

    log_columns = ['season', 'pid', 'gid', 'date', 'matchup',
                   'wl', 'min', 'fgm', 'fga', 'fg_pct',
                   'fg3m', 'fg3a', 'fg3_pct', 'ftm', 'fta',
                   'ft_pct', 'oreb', 'dreb', 'reb', 'ast',
                   'stl', 'blk', 'tov', 'pf', 'pts', 'pm',
                   'vid_available']

    scraped_logs = pd.DataFrame()

    def get_player_info(self, year):
        response = requests.get(self.id_prefix + '/' + str(year) + '/players/10_player_info.json', headers=self.headers)
        df = pd.DataFrame(response.json()['pls']['pl'])
        df = df.drop_duplicates(subset='pid')
        return df

    def get_player_log(self, year, pid):
        if year == 2013:
            response = requests.get(self.log_prefix + str(pid) + '&Season=2013-14&SeasonType=Regular+Season', headers=self.headers)
            df = json_normalize(response.json(), ['resultSets', 'rowSet'])
            if df.empty is False:
                df.columns = self.log_columns
                df['pid'] = df.pid.astype(int)
                return df
            else:
                return df
        elif year == 2014:
            response = requests.get(self.log_prefix + str(pid) + '&Season=2014-15&SeasonType=Regular+Season', headers=self.headers)
            df = json_normalize(response.json(), ['resultSets', 'rowSet'])
            if df.empty is False:
                df.columns = self.log_columns
                df['pid'] = df.pid.astype(int)
                return df
            else:
                return df
        elif year == 2015:
            response = requests.get(self.log_prefix + str(pid) + '&Season=2015-16&SeasonType=Regular+Season', headers=self.headers)
            df = json_normalize(response.json(), ['resultSets', 'rowSet'])
            if df.empty is False:
                df.columns = self.log_columns
                df['pid'] = df.pid.astype(int)
                return df
            else:
                return df
        else:
            response = requests.get(self.log_prefix + str(pid) + '&Season=2016-17&SeasonType=Regular+Season', headers=self.headers)
            df = json_normalize(response.json(), ['resultSets', 'rowSet'])
            if df.empty is False:
                df.columns = self.log_columns
                df['pid'] = df.pid.astype(int)
                return df
            else:
                return df

    def run(self):
        for year in [2013, 2014, 2015, 2016]:
            print(year)
            season_logs = pd.DataFrame(columns=self.log_columns)
            player_info = self.get_player_info(year)
            player_info['player'] = player_info.fn + ' ' + player_info.ln
            player_ids = player_info['pid'].values.tolist()
            for pid in player_ids:
                df = self.get_player_log(year, pid)
                season_logs = season_logs.append(df, ignore_index=True)

            season_logs = season_logs.merge(player_info, on='pid', how='left')
            self.scraped_logs = self.scraped_logs.append(season_logs, ignore_index=True)

        self.scraped_logs.to_csv('./gamelogs.csv', index=False)

#if __name__ == '__main__':

scraper = WNBAScraper()
print("Scraping game logs...")
scraper.run()
