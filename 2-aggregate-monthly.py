import importlib

import argparse
import os
from os import listdir
from os.path import isfile, join
import re

from ConfigTyped import ConfigTyped
from revddit_aggregator import AggregateMonthly
from logger import log


class Launcher():
    def __init__(self, configFile, mode='normal'):
        ct = ConfigTyped(configFile, mode)
        opts = ct.opts

        thisDir = os.path.dirname(os.path.abspath(__file__))
        idir = opts['pushshift_slim_dir']
        onlyfiles = sorted([join(idir, f) for f in listdir(idir) if isfile(join(idir, f))], reverse=True,
                           key = lambda x: os.path.basename(x)[3:])
        log('2-aggregate-monthly')
        for input_file in onlyfiles:
            extension = os.path.splitext(input_file)[1].lower()
            basename = os.path.basename(re.sub(r'\.[^.]*$','',input_file))
            if extension == '.csv':
                output_file = join(opts['aggregate_dir'], basename+'.csv')
                if (not isfile(output_file)) or opts['force']:
                    log(basename)
                    am = AggregateMonthly(input_file, output_file, opts['aggregate_n_rows'], opts['dropna'])
                    am.aggregate()
                    am.write_csv()
        log('finished')

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description = 'Aggregate reddit data that was removed.')
    ap.add_argument('-m', '--mode', type=str, help="Run mode",
                    default='normal')
    ap.add_argument('-c', '--config', type=str, help="Config file", default='config.ini')
    args = ap.parse_args()
    l = Launcher(args.config,
                 mode=args.mode)
