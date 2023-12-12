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

ELASTIC_TYPE_TO_INDEX = {'comments': 'rc', 'posts': 'rs'}

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
    def __init__(self, input_file, output_dir, type, id_field, extra_fields, inaccessible_ids_file, update_inaccessible_ids_file):
        self.input_file = input_file
        self.output_dir = output_dir
        self.output_file = join(self.output_dir, type+'.csv')
        self.type = type
        self.extra_fields = extra_fields
        self.inaccessible_ids_file = inaccessible_ids_file
        self.update_inaccessible_ids_file = update_inaccessible_ids_file
        self.id_field = id_field
        self.url = commentURL if type == 'comments' else postURL
    def process(self):
        new_dfs = []
        ids_archived = {}
        # Do not redownload data for IDs that already exist
        old_df = []
        inaccessible_ids = set()
        if isfile(self.inaccessible_ids_file):
            inaccessible_ids_df = pd.read_csv(self.inaccessible_ids_file)
            inaccessible_ids = set(inaccessible_ids_df['id'])
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
            ids_archived_relookup = dict()
            if old_df_fields != new_df_fields:
                log('ERROR: Fields do not match, move or remove output file before continuing: '+
                    basename(normpath(self.output_dir))+'/'+self.type+'.csv')
                return
            if self.type == 'posts':
                ids_archived = dict.fromkeys(old_df.index)
            else:
                ## Mark comments with blank created_utc as a big date because created_utc can't be blank in next condition
                ## With a big date, they always get re-looked up. There are only 504 blanks
                non_blank_created_utc = old_df['created_utc'].replace('', '12345678901').map(int)
                ## 1609257600 = 2020/12/30
                ##   As of 2021/08, Pushshift now returns [removed] where it once had content for comments. See:
                        #    2021/08/12 - Many old comments (all?) that once returned a body now don't
                        #       https://www.reddit.com/r/pushshift/comments/p21ea6/the_new_beta_api_will_be_going_back_up_in_less/h8mpjwg/
                        #    2021/09/03 - Removed comment bodies older than 24 hours become [removed]
                        #       https://www.reddit.com/r/pushshift/comments/pgzdav/the_api_now_appears_to_rewrite_nearly_all/
                ##  Already retrieved data prior to 2020/12/30, so for everything after that that is [removed],
                ##  don't mark it as existing and keep checking if it exists b/c it might come back later
                ids_archived_condition = ((non_blank_created_utc < 1609257600) | (old_df['body'] != '[removed]'))
                ids_archived = dict.fromkeys(old_df[ids_archived_condition].index)
                ids_archived_relookup = dict.fromkeys(old_df[~ ids_archived_condition].index)
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

        ## Remove IDs that we don't want to look up again, i.e. either:
        ##    - id exists in old_df and isn't a candidate for re-lookup
        ##    - id is in inaccessible_ids (ids for which pushshift does not have data)
        if len(ids_archived):
            ids = [id for id in ids if id not in ids_archived and id not in inaccessible_ids]
        chunks = list(chunk(ids, chunk_size))
        names_not_in_pushshift = set()
        new_inaccessible_ids = set()
        def addResults(results):
            if len(results):
                resdf = pd.DataFrame(list(results.values()))
                resdf.set_index('id',
                                inplace=True,
                                verify_integrity=True,
                                drop=True)
                new_dfs.append(resdf)

        for ids_chunk in tqdm(chunks):
            if USE_ELASTIC:
                requestFunction = lambda: ps_es_queryByID(ELASTIC_TYPE_TO_INDEX[self.type], ids_chunk, self.extra_fields)
            else:
                requestFunction = lambda: ps_api_queryByID(self.url, ids_chunk, self.extra_fields)
            # pushshift_results = loopUntilRequestSucceeds(requestFunction, 'ERROR: Elastic connection failed')
            # Pushshift no longer accessible
            pushshift_results = {}
            found_in_pushshift = set(pushshift_results.keys())
            prefix = 't1_' if self.type == 'comments' else 't3_'
            ## For data missing from pushshift, look up in reddit
            ## Now looking up not only posts, but also comments even a [removed] comment has a post w/a title that gives context
            ## This is possible now b/c of relookup of [removed] items. It wouldn't be good to do this before
            ## because Pushshift appears to be temporarily returning [removed] for content that used to exist,
            ## and writing [removed] by looking these up in reddit would have made that permanent without have this adjusted code.
            names_not_in_pushshift.update([prefix+id for id in ids_chunk if id not in found_in_pushshift])
            addResults(pushshift_results)
        lookup_with_reddit_chunks = list(chunk(list(names_not_in_pushshift), 100))
        for names_chunk in tqdm(lookup_with_reddit_chunks):
            reddit_results = loopUntilRequestSucceeds(lambda: list(reddit.info(names_chunk)), 'ERROR: Reddit connection failed')
            reddit_results_to_add = {}
            for hit in reddit_results:
                row = {'id': hit.id}
                setFieldsForRow(row, hit, self.extra_fields, False, True)
                reddit_results_to_add[hit.id] = row
            new_inaccessible_ids.update([id[3:] for id in names_chunk if id[3:] not in reddit_results_to_add])
            addResults(reddit_results_to_add)
        if self.update_inaccessible_ids_file and len(new_inaccessible_ids):
            new_inaccessible_ids.update(inaccessible_ids)
            inaccessible_ids_df = pd.DataFrame(new_inaccessible_ids,columns=['id'])
            inaccessible_ids_df.sort_values('id', inplace=True)
            inaccessible_ids_df.to_csv(self.inaccessible_ids_file, index=False)
        if len(new_dfs):
            if len(old_df):
                if self.type == 'posts':
                    new_dfs.append(old_df)
                else:
                    new_df_temp_concat = pd.concat(new_dfs, verify_integrity=True)
                    suffix = '_ARCHIVED' ## _ARCHIVED means these columns were already stored on disk. Opposite would be _RE_LOOKED_UP
                    ## Casting as object to avoid adding .0 to the end, making it look like a non-integer to the database
                    new_df_temp_concat['created_utc'] = new_df_temp_concat['created_utc'].astype(object)

                    ## For data that was already archived as [removed], keep everything from newly looked up data.
                    ## If something previously archived is no longer returned, keep the old row we already had.
                    ## Thus, anything previously marked as [removed] is still looked up, and if it later has the body
                    ## then that will overwrite the [removed] record.
                    archived_relookup_merged = new_df_temp_concat.join(old_df[~ ids_archived_condition], how='outer', rsuffix=suffix)
                    ## Copy over data from _ARCHIVED columns where lookup is na
                    for column in old_df.columns:
                        archived_relookup_merged[column] = archived_relookup_merged[column].fillna(archived_relookup_merged[column+suffix])
                    ## Drop _ARCHIVED columns
                    archived_relookup_merged = archived_relookup_merged.filter(regex='^(?!.*'+suffix+')')
                    new_dfs = [archived_relookup_merged, old_df[ids_archived_condition]]
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
