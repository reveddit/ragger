import argparse
import os
from os import listdir
from os.path import isfile, join, getsize
import re

from ConfigTyped import ConfigTyped
from pushshift_file_reader_writer import PushshiftFileProcessor
from files_log import FilesLog, RemoteFileSizes
from exceptions import BadSubmissionData, UnexpectedCompressionFormat
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
        upl = FilesLog(opts['unprocessable_files_log'])
        sl = FilesLog(opts['skippable_files_log'])
        skippable_files = upl.read_entries()+sl.read_entries()
        rfs = RemoteFileSizes(opts['remote_file_sizes'])
        remoteFileSizes = rfs.getSizes()
        for input_file in onlyfiles:
            extension = os.path.splitext(input_file)[1].lower()
            basename = os.path.basename(re.sub(r'\.[^.]*$','',input_file))
            baseWithExt = basename + extension
            # Skip daily files
            if basename.count('-') > 1:
                continue
            if basename in skippable_files:
                continue
            remoteSize = remoteFileSizes.get(baseWithExt)
            localSize = getsize(input_file)
            if remoteSize and remoteSize != localSize:
                sumOfRemoteDailySizes = rfs.getSumOfDaily(basename)
                if sumOfRemoteDailySizes != localSize:
                    log('SIZE MISMATCH:', baseWithExt,
                    '\n                  remote (expected): ', remoteSize,
                    '\n     sum of remote daily (expected): ', sumOfRemoteDailySizes,
                    '\n                  local    (actual): ', localSize)
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
                    def pfp_transform():
                        success = True
                        try_alternate_encoding = False
                        try:
                            pfp.transform()
                        except BadSubmissionData:
                            upl.add_entry(basename)
                            log(basename+' marked as unprocessable')
                            success = False
                        except UnexpectedCompressionFormat as e:
                            log(basename+' ' + str(e))
                            success = False
                            try_alternate_encoding = True
                        return [success, try_alternate_encoding]
                    success, try_alternate_encoding = pfp_transform()
                    if try_alternate_encoding:
                        log(basename+' trying with iso-8859-1')
                        pfp.set_encoding('iso-8859-1')
                        success, _ = pfp_transform()
                    if not success:
                        continue
                        # This continue is not necessary since there is no code after this.
                        # It is here in case that changes in the future
        log('finished')

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description = 'Read Pushshift json files and save a subset of fields as csv.')
    ap.add_argument('-m', '--mode', type=str, help="Run mode",
                    default='normal')
    ap.add_argument('-c', '--config', type=str, help="Config file", default='config.ini')
    args = ap.parse_args()
    l = Launcher(args.config,
                 mode=args.mode)
