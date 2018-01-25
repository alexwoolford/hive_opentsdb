#!/usr/bin/env python

from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
from sqlalchemy.orm import Session
import configparser
import potsdb
import datetime
import delorean


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
        for value_column in self.value_columns:
            for tag_column in self.tag_columns:
                query = select([value_column, self.timestamp_column, tag_column], from_obj=table)
                rows = self.session.execute(query).fetchall()
                for row in rows:
                    epoch_millis = self.get_epoch_millis(row[1])
                    metrics_send_command = '''self.metrics.send("{0}", {1}, timestamp={2}, {3}="{4}")'''.format(value_column, row[0], epoch_millis, tag_column, row[2])
                    eval(metrics_send_command)
        self.metrics.wait()


if __name__ == "__main__":
    hive_opentsdb = HiveOpenTSDB()
    hive_opentsdb.load_hive_to_opentsdb()
