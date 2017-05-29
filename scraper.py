from pandas.io.json import json_normalize
import json
import pandas as pd
import requests

class WNBAScraper:

    headers = {'user-agent': ('Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'),
               'Dnt': ('1'),
               'Accept-Encoding': ('gzip, deflate, sdch'),
               'Accept-Language': ('en'),
               'origin': ('http://stats.nba.com'),
               'Connection': 'close'}
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
        response = requests.get(self.id_prefix + '/' + str(year) + '/players/10_player_info.json', headers=self.headers, stream=False)
        data = json.loads(response.content)
        response.connection.close()
        df = pd.DataFrame(data['pls']['pl'])
        df = df.drop_duplicates(subset='pid')
        return df

    def get_player_log_headers(self, df):
        if df.empty is False:
            df.columns = self.log_columns
            df['pid'] = df.pid.astype(int)
            return df
        else:
            return df

    def get_player_log(self, year, pid):
        response = requests.get(self.log_prefix + str(pid) + '&Season=' + str(year) + '-' + str(int(str(year)[2:4]) + 1) + '&SeasonType=Regular+Season', headers=self.headers)
        data = json.loads(response.content)
        df = json_normalize(data, ['resultSets', 'rowSet'])
        df = self.get_player_log_headers(df)
        return df

    def run(self, years, outfile):
        for year in years:
            print('Scraping game logs from ' + str(year))
            season_logs = pd.DataFrame(columns=self.log_columns)
            player_info = self.get_player_info(year)
            player_info['player'] = player_info.fn + ' ' + player_info.ln
            player_ids = player_info['pid'].values.tolist()
            for pid in player_ids:
                df = self.get_player_log(year, pid)
                season_logs = season_logs.append(df, ignore_index=True)

            season_logs = season_logs.merge(player_info, on='pid', how='left')
            self.scraped_logs = self.scraped_logs.append(season_logs, ignore_index=True)

        self.scraped_logs.to_csv(outfile, index=False)

if __name__ == '__main__':
    scraper = WNBAScraper()
    years = input('Enter desired years in a comma separated list to scrape data:')
    scraper.run(years=years.split(','), outfile='./gamelogs.csv')
