import pandas as pd
import csv
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
import html
import praw
from prawcore.requestor import Requestor

class ModifiedRequestor(Requestor):
    def request(self, *args, **kwargs):
        super().__dict__['_http'].cookies.set('_options', '{%22pref_quarantine_optin%22:true}')
        response = super().request(*args, **kwargs)
        return response

reddit = praw.Reddit(client_id='8x_CT3wS6FugAVYLjLv2Ng',
                     client_secret=None,
                     user_agent='u/rhaksw https://github.com/reveddit/ragger',
                     requestor_class=ModifiedRequestor,
                    )

class MyConnection(Urllib3HttpConnection):
    def __init__(self, *args, **kwargs):
        extra_headers = kwargs.pop('extra_headers', {})
        super(MyConnection, self).__init__(*args, **kwargs)
        self.headers.update(extra_headers)

USE_ELASTIC=True

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

# not working: ElasticSearch({..., 'port': 443, 'use_ssl': True})
#  must be because some package has the wrong version
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
        if hasattr(hit, 'id'):
            # need to delete id here so it doesn't appear in 'hit' for setFieldsForRow
            del hit.id
        row = {'id': id}
        setFieldsForRow(row, hit, fields, True)
        results[id] = row
    return results

def setFieldsForRow(row, hit, fields, ES = False, isRedditObj = False):
    for field in fields:
        if isRedditObj:
            val = getattr(hit, field)
        else:
            val = hit[field]
        if field == 'body' or field == 'title':
            val = re.sub(r'\s+', ' ', html.unescape(val)).strip()[:300]
        elif field == 'link_id':
            if not ES or (val is not None and not isinstance(val, int)):
                val = val[3:]
            else:
                val = toBase36(int(val))
        elif field == 'created_utc':
            ## For praw obj whose type is float
            val = int(val)
        row[field] = val

def loopUntilRequestSucceeds(requestFunction, errorMsg):
    sleepTime=1
    while True:
        try:
            results = requestFunction()
            break
        except Exception as e:
            log(errorMsg + '.', 'Trying again after sleep', sleepTime)
            sleep(sleepTime)
            sleepTime += 1
    return results


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
            # Note: ~150 scores are missing from 3-aggregate_all/RC_ file
            #       To backfill this you'd need to
            #          1) query the reddit API for missing scores
            #          2) change below code to merge scores instead of dropping them
            #   does not seem worthwhile
            old_df = pd.read_csv(
                self.output_file,
                dtype={'created_utc': object, 'score': object},
                index_col='id',
                na_filter=False #when titles or comments == 'null' or 'na', do not mark them as NaN in pandas
            )
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
        # Chunk size can be 800 w/elastic, 500 w/api.pushshift.io which now returns max of 580
        chunk_size = 800 if USE_ELASTIC else 500
        if self.type == 'posts':
            comments_df = pd.read_csv(join(self.output_dir, 'comments.csv'))
            additional_ids = list(comments_df['link_id'])
        ids = list(set(list(df[self.id_field])+additional_ids))

        ## Remove IDs that are in old_df
        if len(existing_ids):
            ids = [id for id in ids if id not in existing_ids]
        chunks = list(chunk(ids, chunk_size))
        names_not_in_pushshift = set()
        def addResults(results):
            if len(results):
                resdf = pd.DataFrame(list(results.values()))
                resdf.set_index('id',
                                inplace=True,
                                verify_integrity=True,
                                drop=True)
                new_dfs.append(resdf)

        for ids_chunk in tqdm(chunks):
            elastic_index = 'rc' if self.type == 'comments' else 'rs'
            sleepTime=1
            if USE_ELASTIC:
                requestFunction = lambda: ps_es_queryByID(elastic_index, ids_chunk, self.extra_fields)
            else:
                requestFunction = lambda: ps_api_queryByID(self.url, ids_chunk, self.extra_fields)
            pushshift_results = loopUntilRequestSucceeds(requestFunction, 'ERROR: Elastic connection failed')
            if self.type == 'posts':
                ## Only looking up missing post IDs with reddit
                ## Pointless to look up missing comment IDs there since body text would all be [removed]
                found = set(pushshift_results.keys())
                names_not_in_pushshift.update(['t3_'+id for id in ids_chunk if id not in found])
            addResults(pushshift_results)
        lookup_with_reddit_chunks = list(chunk(list(names_not_in_pushshift), 100))
        for names_chunk in tqdm(lookup_with_reddit_chunks):
            sleepTime=1
            reddit_results = loopUntilRequestSucceeds(lambda: list(reddit.info(names_chunk)), 'ERROR: Reddit connection failed')
            reddit_results_to_add = {}
            for hit in reddit_results:
                row = {'id': hit.id}
                setFieldsForRow(row, hit, self.extra_fields, False, True)
                reddit_results_to_add[hit.id] = row
            addResults(reddit_results_to_add)
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
            ## Some comment scores are undefined too. See note above "old_df = pd.read_csv()"
            df['score'] = df['score'].astype(object)
            output_df = new_df.join(df[['score']])
            output_df.to_csv(self.output_file)
