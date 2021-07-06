import json
import pandas as pd
import numpy as np
import os.path
import re
from os import listdir, remove
from os.path import isfile, join

from logger import log

def sum_pos(s):
    return s[s>0].sum()

def count_pos(s):
    return s[s>0].count()

def base36_to_int(val):
    return int(str(val), 36)

## After .agg(), id_int_last will be a float
## So, need to cast to int
def int_to_base36(integer):
    if np.isnan(integer):
        return ''
    return np.base_repr(int(integer), 36).lower()

def logMemUsage(df, deep=True):
    mem = df.memory_usage(deep=deep)
    log(str(mem))
    log(str(mem.sum() / (1024**2))+" MB")


class AggregateMonthly():
    def __init__(self, input_file, output_file, n_rows = 1000, dropna = True):
        self.input_file = input_file
        self.output_file = output_file
        self.n_rows = n_rows
        self.dropna = dropna
    def write_csv(self, output_file=''):
        if not output_file:
            output_file = self.output_file
        if self.dropna:
            self.df = self.df.dropna()
        else:
            self.df = self.df.fillna(
                value={'rate':0,
                       'num_pos_upvotes_removed': 0,
                       'num_items_with_pos_upvotes_removed': 0,
                       'id_of_max_pos_removed_item': ''})
        self.df.astype({'rate':float,
                        'num_pos_upvotes_removed': int,
                        'num_items_with_pos_upvotes_removed': int,
                        'score_of_max_pos_removed_item': int}) \
               .to_csv(output_file, index=False)

    def aggregate(self):
        df = pd.read_csv(self.input_file,
                         dtype={'id': 'str',
                                'created_utc': 'uint32',
                                # Note: Don't use 'category' dtype for subreddit.
                                #       Strangely, it uses more memory than 'str', according to `top`:
                                #   with str:
                                #          Max RAM: ~36GB (during first .groupby([subreddit, g]).agg())
                                #                   peak @ 56.8% of 64GB
                                #             Time: 21 mins
                                #   with category:
                                #          Max RAM: ~39GB (during first .groupby([subreddit, g]).agg())
                                #                   peak @ 60% of 64GB
                                #             Time: 22 mins
                                # Another quirk: When using 'category' type, must set observed=True in .groupby() calls that group by a categorical.
                                #                There are currently two .groupby(subreddit) calls in this script
                                'subreddit': 'str',
                                'score': 'int', # Could use int32 here if needed. agg(sum) will auto-convert datatypes to be larger
                                'is_removed': 'bool'})
        ## Dropping dupes here b/c it's easier than keeping track of dupes during file's creation
        ##   Ideally, this would be done when writing the file
        ## File causing the issue: RC_2017-09
        df.drop_duplicates(subset='id', keep='last', inplace=True)
        df['id_int'] = df.id.apply(base36_to_int)
        df.drop('id', axis=1, inplace=True) ## Saves memory to work with int rather than string
        ## Sort input data, don't assume it is already sorted
        df.sort_values(['created_utc', 'id_int'], ascending=[True, True], inplace=True)
        g = df.groupby('subreddit').cumcount() // self.n_rows
        ## If script runs out of memory here, check that subreddit is str
        df_agg = (df.groupby(['subreddit', g], sort=False)
                    .agg({'score': [sum_pos,'count'], 'created_utc':'last', 'id_int':'last'}))
        df.drop(['created_utc'], axis=1, inplace=True)
        ## If script runs out of memory here, check that subreddit is str
        removed_df = (df[(df['is_removed']) & (df['score'] > 1)]
                        .groupby(['subreddit', g], sort=False)['score']
                        .agg(['sum', 'count', 'idxmax', 'max']))
        df_agg.columns = df_agg.columns.map('{0[0]}_{0[1]}'.format)
        ## If rdiv() erorrs with the following:
           ## "cannot handle a non-unique multi-index!"
        ## Check if 'subreddit' was defined as 'category' type on input
        ## Why: B/C running .groupby() on a categorical without setting observed=True results in a lot of duplicated NaN rows
        ## But, even with observed=True, the script runs out of memory as described in the comment under read_csv()
        ## So the solution is to set subreddit as a str.
        ## This error only occurs on test data. Script runs out of memory before getting here on full dataset.
        df_agg['rate'] = df_agg[['score_sum_pos']].rdiv(removed_df['sum'], axis=0)
        df_agg['num_pos_upvotes_removed'] = removed_df['sum']
        df_agg['num_items_with_pos_upvotes_removed'] = removed_df['count']
        df_agg['score_of_max_pos_removed_item'] = removed_df['max']
        df_agg['id_of_max_pos_removed_item'] = removed_df['idxmax'].map(df['id_int'].apply(int_to_base36))
        df_agg.id_int_last = df_agg.id_int_last.apply(int_to_base36)
        d = {'created_utc_last':'last_created_utc','id_int_last':'last_id','score_sum_pos':'total_pos_upvotes','score_count':'total_items'}
        self.df = df_agg.reset_index(level=1, drop=True).reset_index().rename(columns=d)


def combine_rows(a, b):
    if a.last_created_utc < b.last_created_utc:
        x = a; y = b
    else:
        x = b; y = a
    num_pos_upvotes_removed = x.num_pos_upvotes_removed+y.num_pos_upvotes_removed
    total_pos_upvotes = x.total_pos_upvotes+y.total_pos_upvotes
    if x.score_of_max_pos_removed_item > y.score_of_max_pos_removed_item:
        id_of_max_pos_removed_item = x.id_of_max_pos_removed_item
        score_of_max_pos_removed_item = x.score_of_max_pos_removed_item
    else:
        id_of_max_pos_removed_item = y.id_of_max_pos_removed_item
        score_of_max_pos_removed_item = y.score_of_max_pos_removed_item
    return [
        x.subreddit,
        x.total_pos_upvotes+y.total_pos_upvotes,
        x.total_items+y.total_items,
        y.last_created_utc,
        y.last_id,
        (num_pos_upvotes_removed) / (total_pos_upvotes),
        num_pos_upvotes_removed,
        x.num_items_with_pos_upvotes_removed+y.num_items_with_pos_upvotes_removed,
        score_of_max_pos_removed_item,
        id_of_max_pos_removed_item
    ]

agg_all_columns = [
        'subreddit',
        'total_pos_upvotes',
        'total_items',
        'last_created_utc',
        'last_id',
        'rate',
        'num_pos_upvotes_removed',
        'num_items_with_pos_upvotes_removed',
        'score_of_max_pos_removed_item',
        'id_of_max_pos_removed_item'
]



class AggregateAll():
    def __init__(self, input_dir, min_rows = 500, expected_rows = 1000):
        self.input_dir = input_dir
        self.min_rows = min_rows
        self.expected_rows = expected_rows
    def process(self, file_prefix):
        idir = self.input_dir
        files = sorted([join(idir, f)
                                for f in listdir(idir)
                                    if isfile(join(idir, f))
                                        and re.search('^'+file_prefix, f)
                                        and re.search('\.csv$', f)],
                            key = lambda x: os.path.basename(x)[3:])
        df = pd.concat((pd.read_csv(f) for f in files))
        df = df.sort_values(['subreddit', 'last_created_utc'], ascending=[True, True])
        df['subreddit'] = df['subreddit'].str.lower()
        self.df = pd.DataFrame(self.aggregate(df), columns=agg_all_columns)

    def aggregate(self, df):
        prev_row = df.iloc[0]
        new_rows = list()
        prev_row_combined = False
        for i, row in enumerate(df.itertuples(index=False)):
            if (row.total_items < self.min_rows and
               prev_row.total_items == self.expected_rows):
                if i > 0:
                    if prev_row.subreddit == row.subreddit:
                        new_rows.append(combine_rows(prev_row, row))
                        prev_row_combined = True
                        prev_row = row
                        continue
            if i != 0 and not prev_row_combined:
                new_rows.append(prev_row)
            prev_row_combined = False
            prev_row = row
        if not prev_row_combined:
            new_rows.append(prev_row)
        return new_rows

    def write_csv(self, output_file):
        self.df.to_csv(output_file, index=False)
