#!/usr/bin/env python

from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
from sqlalchemy.orm import Session
import configparser
import potsdb
import datetime
import delorean
import sys


class HiveOpenTSDB:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('hive_to_opentsdb.conf')

        hive_host = config.get('hive', 'host')
        hive_port = config.get('hive', 'port')
        hive_db = config.get('hive', 'db')

        self.timestamp_column = config.get('hive', 'timestamp_column')
        self.tag_columns = config.get('hive', 'tag_columns').split(",")
        self.value_columns = config.get('hive', 'value_columns').split(",")

        opentsdb_host = config.get('opentsdb', 'host')
        opentsdb_port = config.get('opentsdb', 'port')

        self.table = config.get('hive', 'table')
        self.engine = create_engine('hive://{0}:{1}/{2}'.format(hive_host, hive_port, hive_db))
        self.session = Session(bind=self.engine)

        self.metrics = potsdb.Client(opentsdb_host, port=opentsdb_port)

    def get_epoch_millis(self, date_raw):
        dt = datetime.datetime.strptime(date_raw, '%Y-%m-%d %H:%M:%S')
        epoch_millis = delorean.Delorean(dt, timezone="UTC").epoch * 1000
        return epoch_millis

    def load_hive_to_opentsdb(self):
        table = Table(self.table, MetaData(bind=self.engine), autoload=True)
        all_columns = self.value_columns + self.tag_columns + [self.timestamp_column]
        query = select(all_columns, from_obj=table)
        rows = self.session.execute(query).fetchall()

        for row in rows:
            try:
                row_dict = dict(zip(all_columns, row))
                epoch_millis = self.get_epoch_millis(row_dict.get(self.timestamp_column))
                for value_column in self.value_columns:
                    tags_dict = dict()
                    for tag_column in self.tag_columns:
                        tags_dict[tag_column] = row_dict[tag_column]

                    tags_string = ", ".join(['''{0}="{1}"'''.format(key, value) for key, value in tags_dict.items()])

                    if not row_dict.get(value_column):
                        row_dict[value_column] = 0

                    metrics_send_command = '''self.metrics.send("{0}", {1}, timestamp={2}, {3})'''.format(value_column,
                                                                                                          row_dict.get(value_column),
                                                                                                          epoch_millis,
                                                                                                          tags_string)
                    self.metrics.send("totalcapacitygb", 8575.0, timestamp=1.493728796e+12, tier="0", hostname="den2s1pure0100", vendor="pure", location="den2")

                    eval(metrics_send_command)
            except:
                # TODO: write error records somewhere
                print sys.exc_info()

        self.metrics.wait()


if __name__ == "__main__":
    hive_opentsdb = HiveOpenTSDB()
    hive_opentsdb.load_hive_to_opentsdb()
