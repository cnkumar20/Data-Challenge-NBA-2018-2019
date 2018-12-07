import pandas as pd
import numpy as np
import json
from scipy import stats

data_df = pd.read_json('players.json',lines=True)
data_df
#get teams configs
json_data=open('teams_config.json').read()
data = json.loads(json_data)
result_filterd_config_list = []
for x in data['teams']['config']:
    filtered_team_config = dict()
    #filterd_team_config['teamId']
    try:
        x['ttsName']
    except:
       continue
    filtered_team_config['teamId'] = x['teamId']
    filtered_team_config['ttsName'] = x['ttsName']
    filtered_team_config['tricode'] = x['tricode']
    result_filterd_config_list.append(filtered_team_config)

#create teams_configs Dataframe
team_configs_pd = pd.DataFrame(result_filterd_config_list)

data_df.head(10)
data_df.count()
data_df.info()

unfiltered_count = new.groupby('teamId').teamId.count()
unfiltered_count.head(30)
unfiltered_count.count()
unfiltered_count.head()
data_df.dropna()
data_df.info()
type(data_df.iloc[0]['heightFeet'])


filtered_data_df = data_df.loc[data_df['heightFeet'] != '-'].copy()
filtered_data_df['heightFeet'] = filtered_data_df.heightFeet.astype(int)
filtered_data_df['heightInches'] = filtered_data_df.heightInches.astype(int)
filtered_data_df.info()
filtered_data_df.sort_values(['heightFeet','heightInches','personId'])

players_pd = filtered_data_df[['heightFeet','heightInches','teamId','personId']].sort_values(['teamId'],ascending=True).copy()


team_configs_pd
#join players and teams config

players_pd.info()
#keep same type
players_pd['teamId'] = players_pd.teamId.astype(int)
players_pd
team_configs_pd
team_configs_pd['teamId'] = team_configs_pd.teamId.astype(int)
team_configs_pd

players_pd.info()
player_team_pd = pd.merge(players_pd,team_configs_pd,on='teamId')

ht_average_team = player_team_pd.groupby('ttsName').mean()[['heightFeet','heightInches']]
ht_average_team['avg_total_ht'] = ht_average_team['heightFeet']*12+ht_average_team['heightInches']

ht_average_team.sort_values('avg_total_ht')
