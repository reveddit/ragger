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

from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles

from ConfigTyped import ConfigTyped

@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"


aggregate_dtype = {
   'subreddit':'category',
   'total_pos_upvotes':'int',
   'total_items':'int',
   'last_created_utc':'int',
   'last_id':'str',
   'rate':'float',
   'num_pos_upvotes_removed':'int',
   'num_items_with_pos_upvotes_removed':'int',
   'score_of_max_pos_removed_item':'int',
   'id_of_max_pos_removed_item':'str'
}

dtype = {
    'aggregate_comments': aggregate_dtype,
    'aggregate_posts': aggregate_dtype,
    'comments': {
        'id': 'str',
        'body': 'str',
        'created_utc': 'int',
        'link_id': 'str',
        'score': 'object' # this could be int, but when re-aggregating after new monthly dump file is added, some old comments dont exist anymore in the RC_aggregate_all file. there's probably a way to omit these comments, but setting the field to 'nullable' by typing it as object is easier for now
    },
    'posts': {
        'id': 'str',
        'created_utc': 'int',
        'num_comments': 'int',
        'title': 'str',
        'score': 'object'
    }
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

        engine = create_engine(dbconfig.get_connectString_for_user(dbopts['other_db_name']), pool_pre_ping=True)
        with engine.connect() as con:
            con.execute('COMMIT;')
            con.execute(
                f"DROP DATABASE IF EXISTS {dbopts['db_tmp_name']};"
            )
            con.execute('COMMIT;')
            con.execute(
                f"CREATE DATABASE {dbopts['db_tmp_name']};"
            )

        chunksize = 10**2 # Digital Ocean 1 GB ram droplet can handle 10**5
        for table, input_file in table_files.items():
            engine = create_engine(dbconfig.get_connectString_for_user(dbopts['db_tmp_name']), pool_pre_ping=True)
            log(table)
            if_exists = 'replace'
            for chunk in pd.read_csv(input_file,
                                     dtype=dtype[table],
                                     usecols=list(dtype[table].keys()),
                                     chunksize=chunksize):
                chunk.to_sql(table, engine, if_exists=if_exists, index=False)
                if_exists = 'append'
        log('finished. run 6-create-db-functions.py.  might need to reload hasura meta via api.revddit.com/console')



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
