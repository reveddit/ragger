import argparse
import os
from os import listdir
from os.path import isfile, join
import re

from ConfigTyped import ConfigTyped
from pushshift_file_reader_writer import PushshiftFileProcessor
from files_log import FilesLog
from exceptions import BadSubmissionData
from logger import log

class Launcher():
    def __init__(self, configFile, mode='normal'):
        ct = ConfigTyped(configFile, mode)
        opts = ct.opts

        thisDir = os.path.dirname(os.path.abspath(__file__))
        idir = opts['pushshift_raw_dir']
        onlyfiles = sorted([join(idir, f) for f in listdir(idir) if isfile(join(idir, f))], reverse=True,
                           key = lambda x: os.path.basename(x)[3:])
        log('1-pushshift-slim')
        for input_file in onlyfiles:
            extension = os.path.splitext(input_file)[1].lower()
            basename = os.path.basename(re.sub(r'\.[^.]*$','',input_file))
            upl = FilesLog(opts['unprocessable_files_log'])
            sl = FilesLog(opts['skippable_files_log'])
            if basename in (upl.read_entries()+sl.read_entries()):
                continue
            type = 'comments'
            if basename[1].lower() == 's':
                type = 'posts'
            if extension in ['.gz', '.xz', '.zst', '.bz2']:
                output_file = join(opts['pushshift_slim_dir'], basename+'.csv')
                if (not isfile(output_file)) or opts['force']:
                    log(basename)
                    pfp = PushshiftFileProcessor(input_file,
                                                 output_file,
                                                 type,
                                                 opts['max_lines_to_read'])
                    try:
                        pfp.transform()
                    except BadSubmissionData:
                        upl.add_entry(basename)
                        log(basename+' marked as unprocessable')
                        continue
        log('finished')

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description = 'Read Pushshift json files and save a subset of fields as csv.')
    ap.add_argument('-m', '--mode', type=str, help="Run mode",
                    default='normal')
    ap.add_argument('-c', '--config', type=str, help="Config file", default='config.ini')
    args = ap.parse_args()
    l = Launcher(args.config,
                 mode=args.mode)
