from bz2 import BZ2File
import lzma
import zstandard as zstd
import json
from itertools import zip_longest
import os.path
import os
import re

from exceptions import BadSubmissionData
from logger import log

columns = ['id', 'created_utc', 'subreddit', 'score', 'is_removed']

def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


class PushshiftFileProcessor():
    def __init__(self, input_file, output_file, type, max_lines_to_read=0):
        self.input_file = input_file
        self.output_file = output_file
        if type not in ['comments', 'posts']:
            raise Exception('Bad Pushshift file type: ['+type+']')
        self.type = type
        self.max_lines_to_read = max_lines_to_read
        self.data = []

    def transform(self):
        extension = os.path.splitext(self.input_file)[1]
        if extension == '.bz2':
            with BZ2File(self.input_file) as infile:
                self.transform_xz_bz_file(infile)
        if extension == '.xz':
            with lzma.open(self.input_file, 'r') as infile:
                self.transform_xz_bz_file(infile)
        elif extension == '.zst':
            with open(self.input_file, 'rb') as infile:
                self.transform_zst_file(infile)
        self.write_csv()

    def write_csv(self):
        with open(self.output_file, 'w') as f:
            f.write(','.join(columns)+"\n")
            for item in self.data:
                f.write(','.join(map(str,item))+"\n")

    def transform_xz_bz_file(self, infile):
        # 44m24s run time for 104,473,929 comments stored on HD [RC_2018-09.xz, 10GB]
        xz_bz2_num_lines = 10000
        lines_read = 0
        for i,lines in enumerate(grouper(infile, xz_bz2_num_lines, '')):
            for line in lines:
                if line.strip():
                    self.appendData(line, self.type)
                    lines_read += 1
                    if self.max_lines_to_read and lines_read >= self.max_lines_to_read:
                        return

    def transform_zst_file(self, infile):
        zst_num_bytes = 2**22
        lines_read = 0
        # 34m39s run time for 121,953,600 comments stored on HD [RC_2018-12.zst, 12GB]
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(infile) as reader:
            previous_line = ""
            while True:
                chunk = reader.read(zst_num_bytes)
                if not chunk:
                    break
                string_data = chunk.decode('utf-8')
                lines = string_data.split("\n")
                for i, line in enumerate(lines[:-1]):
                    if i == 0:
                        line = previous_line + line
                    self.appendData(line, self.type)
                    lines_read += 1
                    if self.max_lines_to_read and lines_read >= self.max_lines_to_read:
                        return
                previous_line = lines[-1]

    def appendData(self, line, type):
        x = json.loads(line)
        is_removed = False
        if type == 'comments':
            if x['body'].replace('\\','') == '[removed]' and x['author'].replace('\\','') == '[deleted]':
                is_removed = True
        else:
            if x['author'].replace('\\','') != '[deleted]':
                if 'is_robot_indexable' in x:
                    if not x['is_robot_indexable']:
                        is_removed = True
                elif 'is_crosspostable' not in x:
                    raise BadSubmissionData('missing key is_crosspostable: '+x['id'])
                elif not x['is_crosspostable']:
                    is_removed = True
        ## fix for RC_2017-11 and some RS file
        if x['score'] is None:
            x['score'] = 1
        try:
            self.data.append([x['id'], x['created_utc'], x['subreddit'], x['score'], is_removed])
        except:
            ## Below identifies ignorable data and skips over it
            ## If something unexpected is found, raise Exception
            try:
                if x['permalink'][:10] == '/comments/' or re.match(r'^/r/u_'+x['author']+'/', x['permalink']):
                    return
            except:
                pass
            try:
                if x['promoted']:
                    return
            except:
                pass
            raise Exception('Unexpected key error for id ['+x['id']+']')
