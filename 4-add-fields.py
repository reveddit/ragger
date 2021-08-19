import importlib

import argparse
import os
from os import listdir
from os.path import isfile, join
import re

from ConfigTyped import ConfigTyped
from revddit_aggregator import AggregateAll
from logger import log
from AddFields import AddFields

class Launcher():
    def __init__(self, configFile, mode='normal', overwrite = False):
        ct = ConfigTyped(configFile, mode)
        opts = ct.opts
        thisDir = os.path.dirname(os.path.abspath(__file__))
        idir = opts['aggregate_all_dir']
        types = {'RC_': 'comments', 'RS_': 'posts'}
        ## Order matters, must complete 'RC_' first
        for prefix in ['RC_', 'RS_']:
            type = types[prefix]
            file = join(idir, prefix+'aggregate_all.csv')
            log('4-add-fields '+type)
            af = AddFields(file,
                           opts['add_fields_dir'],
                           type,
                           opts['add_fields_id_field'],
                           list(map(lambda x: x.strip(), opts['extra_fields_'+type].split(','))),
                           opts['inaccessible_ids_file_'+type],
                           overwrite,
                          )
            af.process()
        log('finished')

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description = 'Add back fields not needed for earlier processing.')
    ap.add_argument('-m', '--mode', type=str, help="Run mode",
                    default='normal')
    ap.add_argument('-c', '--config', type=str, help="Config file", default='config.ini')
    ap.add_argument('-w', '--overwrite', action='store_true', default=False, help='Overwrite inaccessible ids files.')
    args = ap.parse_args()
    l = Launcher(args.config,
                 mode=args.mode,
                 overwrite=args.overwrite,
                 )
