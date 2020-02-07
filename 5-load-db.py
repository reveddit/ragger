import importlib

import argparse
import os
from os import listdir
from os.path import isfile, join
import re
import time
import shutil
from sqlalchemy import create_engine

import pandas as pd
from logger import log
import sys

from sqlalchemy.schema import DropTable, CreateSchema
from sqlalchemy.ext.compiler import compiles

import psycopg2

from ConfigTyped import ConfigTyped

@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"

reddit_id_column = 'char(15)'

aggregate_columns = f"""

    subreddit char(30) NOT NULL,
    total_pos_upvotes bigint NOT NULL,
    total_items bigint NOT NULL,
    last_created_utc bigint NOT NULL,
    last_id {reddit_id_column} NOT NULL,
    rate double precision NOT NULL,
    num_pos_upvotes_removed bigint NOT NULL,
    num_items_with_pos_upvotes_removed bigint NOT NULL,
    score_of_max_pos_removed_item bigint NOT NULL,
    id_of_max_pos_removed_item {reddit_id_column} NOT NULL

"""

# [comments|posts].score could be non-nullable, but when re-aggregating
# after new monthly dump file is added, some old items dont exist anymore
# in the R*_aggregate_all file. There is probably a way to omit these items,
# but setting it to be nullable is easier for now
columns = {
    'aggregate_comments': aggregate_columns,
    'aggregate_posts': aggregate_columns,
    'comments': f"""
        id {reddit_id_column} NOT NULL,
        body char(300),
        created_utc bigint NOT NULL,
        link_id {reddit_id_column} NOT NULL,
        score bigint
    """,
    'posts': f"""
        id {reddit_id_column} NOT NULL,
        created_utc bigint NOT NULL,
        num_comments bigint NOT NULL,
        title char(300),
        score bigint
    """
}

class Launcher():
    def __init__(self, configFile, dbConfigFile, mode='normal'):
        config = ConfigTyped(configFile, mode)
        opts = config.opts
        dbconfig = ConfigTyped(dbConfigFile, mode)
        dbopts = dbconfig.opts
        self.opts = opts
        self.dbopts = dbopts
        thisDir = os.path.dirname(os.path.abspath(__file__))
        aa_dir = opts['aggregate_all_dir']
        af_dir = opts['add_fields_dir']
        table_files = {
            'aggregate_comments': join(aa_dir, 'RC_aggregate_all.csv'),
            'aggregate_posts': join(aa_dir, 'RS_aggregate_all.csv'),
            'comments': join(af_dir, 'comments.csv'),
            'posts': join(af_dir, 'posts.csv'),
        }
        missing_files = [f for f in table_files.values() if not isfile(f)]
        if len(missing_files):
            log('ERROR: missing files: '+str(missing_files))
            sys.exit()
        log('5-load-db')

        other_engine = create_engine(dbconfig.get_connectString(dbopts['other_db_name']), pool_pre_ping=True)
        with other_engine.connect() as con:
            con.execute('COMMIT;')
            result = con.execute(
                f"SELECT 1 FROM pg_database WHERE datname='{dbopts['db_name']}'"
            )
            if (not result.fetchone()):
                con.execute(
                    f"CREATE DATABASE {dbopts['db_name']};"
                )
        engine = create_engine(dbconfig.get_connectString(dbopts['db_name']), pool_pre_ping=True)
        if not engine.dialect.has_schema(engine, dbopts['db_schema_tmp']):
            engine.execute(CreateSchema(dbopts['db_schema_tmp']))
        else:
            with engine.connect() as con:
                con.execute(
                    f"""
                        DROP SCHEMA {dbopts['db_schema_tmp']} CASCADE;
                    """
                )
            engine.execute(CreateSchema(dbopts['db_schema_tmp']))
        for table, input_file in table_files.items():
            log(table)
            conn = psycopg2.connect(dbconfig.get_connectString_psy(dbopts['db_name']))
            cur = conn.cursor()
            cur.execute(
                f"""
                    CREATE TABLE {dbopts['db_schema_tmp']}.{table} ({columns[table]});
                """
            )
            conn.commit()
            with open(input_file, 'r') as f:
                next(f) # Skip the header row
                cur.copy_expert(
                    f"""
                        COPY {dbopts['db_schema_tmp']}.{table} FROM STDIN WITH CSV
                    """, f
                )
                conn.commit()
            cur.close()
            conn.close()
        log('5-load-db finished.')



if __name__ == '__main__':
    ap = argparse.ArgumentParser(description = 'Load data into database.')
    ap.add_argument('-m', '--mode', type=str, help="Run mode",
                    default='normal')
    ap.add_argument('-c', '--config', type=str, help="Config file", default='config.ini')
    ap.add_argument('-d', '--dbconfig', type=str, help="Database config file", default='dbconfig.ini')
    args = ap.parse_args()
    l = Launcher(args.config,
                 args.dbconfig,
                 mode=args.mode)
