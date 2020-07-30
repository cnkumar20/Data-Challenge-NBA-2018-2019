from dataScraper.Loader import *
import time as t
a = Loader()
a.read("leagueRosterPlayers")
t.sleep(10)
a.read('teamsConfig')
t.sleep(10)
a.read('leagueStandings')