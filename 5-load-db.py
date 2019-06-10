import importlib

import argparse
import configparser
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
        'score': 'int'
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
    def __init__(self, configFile, mode='normal'):
        config = configparser.ConfigParser()
        config.read(configFile)
        opts = {}
        for g in ['default', mode]:
            for o in config.options(g):
                opts[o] = config.get(g, o)
                if opts[o].isdigit():
                    opts[o] = config.getint(g, o)
                elif opts[o].lower() in ['true', 'false']:
                    opts[o] = config.getboolean(g, o)
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
        engine = create_engine('postgresql://'+
                                opts['db_user']+':'+
                                opts['db_pw']+'@localhost:'+
                                str(opts['db_port'])+'/'+
                                opts['db_name'])
        chunksize = 10**5 # Digital Ocean 1 GB ram droplet can handle this
        for table, input_file in table_files.items():
            log(time.strftime("%Y-%m-%d %H:%M  ")+table)
            if_exists = 'replace'
            for chunk in pd.read_csv(input_file,
                                     dtype=dtype[table],
                                     usecols=list(dtype[table].keys()),
                                     chunksize=chunksize):
                chunk.to_sql(table, engine, if_exists=if_exists, index=False)
                if_exists = 'append'
        with engine.connect() as con:
            con.execute(
                'CREATE INDEX ix_subreddit_c ON aggregate_comments USING btree (subreddit);'
                'CREATE INDEX ix_subreddit_p ON aggregate_posts USING btree (subreddit);'

                'CREATE INDEX ix_utc_desc_c ON aggregate_comments USING btree (last_created_utc DESC NULLS LAST);'
                'CREATE INDEX ix_utc_desc_p ON aggregate_posts USING btree (last_created_utc DESC NULLS LAST);'

                'CREATE INDEX ix_rate_desc_c ON aggregate_comments USING btree (rate DESC NULLS LAST);'
                'CREATE INDEX ix_rate_desc_p ON aggregate_posts USING btree (rate DESC NULLS LAST);'

                'ALTER TABLE comments ADD PRIMARY KEY (id);'
                'ALTER TABLE posts ADD PRIMARY KEY (id);'
            )
        log('finished')
        ## Not using these two foreign keys b/c aggregate_ tables may not have
        ## corresponding comments/posts entries for data that couldn't
        ## be downloaded from pushshift via the AddFields process.
        ## As a result, when pulling data, aggregate_ tables have some scores that don't appear
        ## in comments/posts, so scores should be pulled from aggregate_ table
        ## So, the score field copied to comments/posts is not used now, and left as-is in case needed later

        #ALTER TABLE aggregate_comments ADD CONSTRAINT fk_mpri_id_c FOREIGN KEY (id_of_max_pos_removed_item) REFERENCES comments (id);
        #ALTER TABLE aggregate_posts ADD CONSTRAINT fk_mpri_id_p FOREIGN KEY (id_of_max_pos_removed_item) REFERENCES posts (id);

        ## Similarly, b/c PS queries fail to download some posts data, this constraint isn't possible to include,

        # 'ALTER TABLE comments ADD CONSTRAINT fk_post_id FOREIGN KEY (link_id) REFERENCES posts (id);'



if __name__ == '__main__':
    ap = argparse.ArgumentParser(description = 'Load or append data to database.')
    ap.add_argument('-m', '--mode', type=str, help="Run mode",
                    default='normal')
    ap.add_argument('-c', '--config', type=str, help="Config file", default='config.ini')
    args = ap.parse_args()
    l = Launcher(args.config,
                 mode=args.mode)
