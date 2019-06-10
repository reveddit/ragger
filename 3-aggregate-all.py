import importlib

import argparse
import os
from os import listdir
from os.path import isfile, join
import re

from ConfigTyped import ConfigTyped
from revddit_aggregator import AggregateAll
from logger import log


class Launcher():
    def __init__(self, configFile, mode='normal'):
        ct = ConfigTyped(configFile, mode)
        opts = ct.opts
        thisDir = os.path.dirname(os.path.abspath(__file__))
        idir = opts['aggregate_dir']
        for prefix in ['RC_', 'RS_']:
            log('3-aggregate-all '+prefix)
            aa = AggregateAll(idir, opts['aggregate_all_min_rows'], opts['aggregate_n_rows'])
            aa.process(prefix)
            output_file = join(opts['aggregate_all_dir'], prefix+'aggregate_all.csv')
            aa.write_csv(output_file)
        log('finished')

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description = 'Aggregate reddit data that was removed.')
    ap.add_argument('-m', '--mode', type=str, help="Run mode",
                    default='normal')
    ap.add_argument('-c', '--config', type=str, help="Config file", default='config.ini')
    args = ap.parse_args()
    l = Launcher(args.config,
                 mode=args.mode)
