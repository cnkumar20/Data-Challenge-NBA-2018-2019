def create_teams_config():
    prefix_url_source="http://data.nba.net/10s"
    team_configs_url=self.prefix_url_source+"/prod/2018/teams_config.json"
    team_config = requests.request(team_configs_url)
    print(team_config.data)
    team_config

    
