import pandas as pd
import re
from os.path import join, basename, isfile, normpath
import requests
from time import sleep
from tqdm import tqdm
from logger import log
import sys
from simplejson.errors import JSONDecodeError
from elasticsearch import Elasticsearch, Urllib3HttpConnection
from elasticsearch_dsl import Search

class MyConnection(Urllib3HttpConnection):
    def __init__(self, *args, **kwargs):
        extra_headers = kwargs.pop('extra_headers', {})
        super(MyConnection, self).__init__(*args, **kwargs)
        self.headers.update(extra_headers)



postURL = 'https://api.pushshift.io/reddit/submission/search/'
commentURL = 'https://api.pushshift.io/reddit/comment/search/'


def toBase10(str):
    return int(str, 36)

def toBase36(number, alphabet='0123456789abcdefghijklmnopqrstuvwxyz'):
    base36 = ''
    sign = ''

    if number < 0:
        sign = '-'
        number = -number

    if 0 <= number < len(alphabet):
        return sign + alphabet[number]

    while number != 0:
        number, i = divmod(number, len(alphabet))
        base36 = alphabet[i] + base36

    return sign + base36

def chunk(arr, n):
    for i in range(0, len(arr), n):
        yield arr[i:i + n]

def ps_api_queryByID(url, ids, fields):
    params={'ids': ','.join(ids),
            'fields':','.join(['id']+fields)}
    params_str = "&".join("%s=%s" % (k,v) for k,v in params.items())
    response = requests.get(url=url, params=params_str)
    try:
        data = response.json()
    except JSONDecodeError:
        log(response.request.url)
        log(response.text)
        sys.exit()

    results = {}
    for hit in data['data']:
        id = hit['id']
        ## Skipping dupes e.g. https://api.pushshift.io/reddit/comment/search/?ids=d9g52ge
        ## Not skipping comment rows whose body [removed] b/c can get full titles from their link_id
        if (id in results and
            'body' in results[id] and
            results[id]['body'].replace('\\','') != '[removed]'):
            continue
        row = {'id': hit['id']}
        setFieldsForRow(row, hit, fields, False)
        results[id] = row
    return results

def ps_es_queryByID(index, ids, fields):
    es = Elasticsearch([{'host': 'elastic.pushshift.io', 'port': 80}],
                       connection_class=MyConnection,
                       extra_headers = {'Referer': 'https://revddit.com'},
                       send_get_body_as='source',
                       timeout=30,
                       max_retries=3)
    ids_base10 = list(map(toBase10, ids))
    s = Search(using=es, index=index) \
        .filter('terms', _id=ids_base10) \
        .source(includes=fields)
    response = s[0:len(ids)].execute()
    results = {}
    for hit in response:
        id = toBase36(int(hit.meta.id))
        row = {'id': id}
        setFieldsForRow(row, hit, fields, True)
        results[id] = row
    return results

def setFieldsForRow(row, hit, fields, ES = False):
    for field in fields:
        val = hit[field]
        if field == 'body':
            val = re.sub(r'\s+', ' ', val).strip()[:300]
        elif field == 'link_id':
            if not ES:
                val = val[3:]
            else:
                val = toBase36(int(val))
        row[field] = val


class AddFields():
    def __init__(self, input_file, output_dir, type, id_field, extra_fields):
        self.input_file = input_file
        self.output_dir = output_dir
        self.output_file = join(self.output_dir, type+'.csv')
        self.type = type
        self.extra_fields = extra_fields
        self.id_field = id_field
        self.url = commentURL if type == 'comments' else postURL
    def process(self):
        new_dfs = []
        existing_ids = {}
        # Do not redownload data for IDs that already exist
        old_df = []
        if isfile(self.output_file):
            old_df = pd.read_csv(self.output_file, dtype={'created_utc': object}, index_col='id')
            # Drop score here b/c it is added later from 3-aggregate_all/ file
            old_df.drop(columns='score', inplace=True, errors='ignore')
            # Make sure extra_fields match in existing data
            old_df_fields = sorted(list(old_df.columns))
            new_df_fields = sorted(self.extra_fields)
            if old_df_fields != new_df_fields:
                log('ERROR: Fields do not match, move or remove output file before continuing: '+
                    basename(normpath(self.output_dir))+'/'+self.type+'.csv')
                return
            existing_ids = dict.fromkeys(old_df.index)
        df = pd.read_csv(self.input_file)
        # Verify uniqueness of previous data
        df.set_index(self.id_field,
                     inplace=True,
                     verify_integrity=True,
                     drop=False)
        additional_ids = []
        chunk_size = 800
        if self.type == 'posts':
            comments_df = pd.read_csv(join(self.output_dir, 'comments.csv'))
            additional_ids = list(comments_df['link_id'])
        ids = list(set(list(df[self.id_field])+additional_ids))

        ## Remove IDs that are in old_df
        if len(existing_ids):
            ids = [id for id in ids if id not in existing_ids]
        chunks = list(chunk(ids, chunk_size))
        for ids_chunk in tqdm(chunks):
            if self.type == 'comments':
                results = ps_api_queryByID(self.url, ids_chunk, self.extra_fields)
                #results = ps_es_queryByID('rc', ids_chunk, self.extra_fields)
            else:
                results = ps_es_queryByID('rs', ids_chunk, self.extra_fields)
            if len(results):
                resdf = pd.DataFrame(list(results.values()))
                resdf.set_index('id',
                                inplace=True,
                                verify_integrity=True,
                                drop=True)
                new_dfs.append(resdf)
        print()
        if len(new_dfs):
            if len(old_df):
                new_dfs.append(old_df)
            new_df = pd.concat(new_dfs, verify_integrity=True)
            df.rename(columns={'score_of_max_pos_removed_item': 'score'}, inplace=True)
            df.index.rename('id', inplace=True)
            ## For many posts, score will be undefined, because we also look up
            ## titles for comments' posts, and these posts may not have been among
            ## results of the previous analysis & aggregation process to find
            ## "top removed items in periods of 1,000 items"
            ## It would be possible to go back and read the processed pushshift monthly
            ## file data to get this information, but I don't have plans to use these
            ## scores anyway. When displaying comments, only the comment score and post
            ## title is shown. Post score is not needed in that case
            df['score'] = df['score'].astype(object)
            output_df = new_df.join(df[['score']])
            output_df.to_csv(self.output_file)
