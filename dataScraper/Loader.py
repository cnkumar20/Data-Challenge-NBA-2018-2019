import requests
import logging
import json
import avro.datafile
import avro.schema
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import os
import sys
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
        leagueRosterPlayers=requests.get(self.url)
        try:
            #print(leagueRosterPlayers.text)
            if(leagueRosterPlayers.status_code!=200):
                raise Exception
        except Exception as error:
            logging.error(repr(error))
        return json.loads(leagueRosterPlayers.text)

    def get_schema(self,key):
        return avro.schema.Parse(open("data/schema/player.avsc", "rb").read())
        

    def convert_json_to_avro():
        avro_schema = avro.schema.make_avsc_object(schema_dict, avro.schema.Names())
        serializer = AvroJsonSerializer(avro_schema)

    def read(self,key):
        self.get_latest_urls()
        key_json_data = self.get_data_from_links_json(self,key)
        schema = get_schema(key)
        key_avro_data = convert_json_to_avro(key_json_data,schema)
