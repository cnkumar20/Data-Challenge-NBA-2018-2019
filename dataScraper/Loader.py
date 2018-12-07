import requests
import io
import logging
import json
import avro.datafile
import avro.schema
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import os
import sys
import datetime
from collections import defaultdict
from avro import schema, datafile, io

"""

"""
class Loader(object):
    start_source_url=None
    prefix_url_source=None
    load_destination=None
    json_data=None
    schema_dir=None
    url =None

    """docstring for ."""
    def __init__(self):
        self.prefix_url_source="http://data.nba.net/10s"
        self.start_source_url=self.prefix_url_source+"/prod/v1/today.json"
        print(self.start_source_url)
        self.schema_dir=""
    def write():
        pass


    """ Read from prefix url source"""
    def get_latest_urls(self):
        try:
            today_url= requests.get(self.start_source_url)
            if(today_url!=200):
                raise Exception("{} not reachable".format(self.prefix_url_source))
        except Exception as error:
            logging.error(repr(error))
        self.json_data = json.loads(today_url.text)
        return self.json_data

    def get_data_from_latest_url(key):
        url=self.json_data[key]
        url = self.prefix_url_source+url
        data = requests.get(url)
        try:
            if(today_url!=200):
                raise Exception("{} not reachable".format(url))
        except Exception as error:
            logging.error(repr(error))
        return data

    def get_data_from_links_json(self,key):
        self.url=self.prefix_url_source+self.json_data["links"][key]
        print(self.url)
        leagueRosterPlayers=requests.get(self.url)
        try:
            #print(leagueRosterPlayers.text)
            if(leagueRosterPlayers.status_code!=200):
                raise Exception
        except Exception as error:
            logging.error(repr(error))
        return json.loads(leagueRosterPlayers.text)

    def get_schema(self,key):
        return avro.schema.Parse(open("./data/schema/player.avsc", "rb").read())


    def create_player_avro_file(self,key,key_json_data,schema):
        batch=20
        count=0
        OUTFILE_NAME="dataScraper/output/player/"+key+"_"+str(datetime.datetime.now().date())+".avro"
        avro_record=[]
        date = datetime.datetime.now().date()
        rec_writer = io.DatumWriter(schema)
        writer = datafile.DataFileWriter(open(OUTFILE_NAME, 'w'),rec_writer,writer_schema=schema,codec='deflate')
        writer_json = open("dataScraper/output/player/players.json",'w')
        json_data = list()
        try:
            for player in key_json_data["league"]["standard"]:
                json_record = player
                count +=1
                result_dict=defaultdict(dict)
                result_dict["firstName"]=player["firstName"]
                result_dict["lastName"]=player["lastName"]
                result_dict["personId"]=player["personId"]
                result_dict["teamId"]=player["teamId"]
                result_dict["jersey"]=player["jersey"]
                result_dict["isActive"]=player["isActive"]
                result_dict["pos"]=player["pos"]
                result_dict["heightFeet"]=player["heightFeet"]
                result_dict["heightInches"]=player["heightInches"]
                result_dict["heightMeters"]=player["heightMeters"]
                result_dict["weightPounds"]=player["weightPounds"]
                result_dict["weightKilograms"]=player["weightKilograms"]
                result_dict["dateOfBirthUTC"]=player["dateOfBirthUTC"]
                writer.append(result_dict)
                json_data.append(result_dict)
            print(count)
            writer.close()
            json.dump(json_data,writer_json)
        except Exception as e:
            print(e)

    def create_teams_config(self):
        try:
            self.prefix_url_source="http://data.nba.net/10s"
            team_configs_url=self.prefix_url_source+"/prod/2018/teams_config.json"
            team_config = requests.get(team_configs_url)
            writer = open("teamsConfig.json",'w')
            writer.write(team_config.text)
            writer.close()
        except:
            print("Exception1111")

    def read_league_standings(self):
        self.prefix_url_source="http://data.nba.net/10s"
        team_configs_url=self.prefix_url_source+"/prod/v2/current/standings_conference.json"
        league = requests.get(team_configs_url)
        writer = open("league.json",'w')
        writer.write(league.text)
        writer.close()

    def read(self,key):
        #self.get_latest_urls()
        #key_json_data = self.get_data_from_links_json(key)
        if(key is "leagueRosterPlayers"):
            self.create_player_avro_file(key,key_json_data,self.get_schema(key))
        elif(key is 'teamsConfig'):
            self.create_teams_config()
        elif(key is 'leagueStandings'):
            self.read_league_standings()
        #    self.read_file(open("output/player/file.avro",'wb'))
            #self.create_player_files_size_n(key,key_json_data,self.get_schema(key))
            #self.push_to_kafka()
