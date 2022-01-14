import argparse
import configparser
from sqlalchemy import create_engine
from logger import log
import sys
import re

from ConfigTyped import ConfigTyped

indexes = [
    'CREATE INDEX ix_subreddit_c ON aggregate_comments USING btree (subreddit);',
    'CREATE INDEX ix_subreddit_p ON aggregate_posts USING btree (subreddit);',
    'CREATE INDEX ix_utc_desc_c ON aggregate_comments USING btree (last_created_utc DESC NULLS LAST);',
    'CREATE INDEX ix_utc_desc_p ON aggregate_posts USING btree (last_created_utc DESC NULLS LAST);',
    'CREATE INDEX ix_rate_desc_c ON aggregate_comments USING btree (rate DESC NULLS LAST);',
    'CREATE INDEX ix_rate_desc_p ON aggregate_posts USING btree (rate DESC NULLS LAST);',
    'ALTER TABLE comments ADD PRIMARY KEY (id);',
    'ALTER TABLE posts ADD PRIMARY KEY (id);',
    'ALTER TABLE aggregate_comments ADD PRIMARY KEY (last_id);',
    'ALTER TABLE aggregate_posts ADD PRIMARY KEY (last_id);',
]

TEXT = 'TEXT'
DOUBLE = 'DOUBLE PRECISION'
BIGINT = 'BIGINT'

commentColumns = [
    ['subreddit', TEXT],
    ['rate', DOUBLE],
    ['last_created_utc', BIGINT],
    ['id_of_max_pos_removed_item', TEXT],
    ['last_id', TEXT],
    ['total_items', BIGINT],
    ['comments.body', TEXT],
    ['comments.created_utc', BIGINT],
    ['comments.link_id', TEXT],
    ['posts.title', TEXT],
    ['score_of_max_pos_removed_item', BIGINT],
]

postColumns = [
    ['subreddit', TEXT],
    ['rate', DOUBLE],
    ['last_created_utc', BIGINT],
    ['id_of_max_pos_removed_item', TEXT],
    ['last_id', TEXT],
    ['total_items', BIGINT],
    ['posts.title', TEXT],
    ['posts.num_comments', BIGINT],
    ['posts.created_utc', BIGINT],
    ['score_of_max_pos_removed_item', BIGINT],
]

def generateSQL(columns):
    columnsReturnTypes = ''
    columnsSelectSQL = ''
    for i, c in enumerate(columns):
        return_name = re.sub(r'.*\.', '', c[0])
        if c[0] == 'score_of_max_pos_removed_item':
            return_name = 'score'
            columnsSelectSQL += f'{c[0]} AS {return_name}'
        elif c[1] == TEXT:
            columnsSelectSQL += f'TRIM({c[0]}) AS '+ return_name
        else:
            columnsSelectSQL += f'{c[0]}'
        columnsReturnTypes += return_name + ' ' + c[1]
        if i < len(columns)-1:
            columnsReturnTypes += ",\n"
            columnsSelectSQL += ",\n"
    return columnsReturnTypes, columnsSelectSQL

commentColumnsReturnTypes, commentColumnsSelectSQL = generateSQL(commentColumns)
postColumnsReturnTypes, postColumnsSelectSQL = generateSQL(postColumns)

functionParamSQL = f"""
    subreddit VARCHAR(30),
    num_records integer,
    created_before bigint default NULL,
    created_after bigint default NULL,
    rate_less double precision default NULL,
    rate_more double precision default NULL
"""

commentFromJoinSQL = f"""
FROM      aggregate_comments
LEFT JOIN comments
ON        aggregate_comments.id_of_max_pos_removed_item = comments.id
LEFT JOIN posts
ON        comments.link_id = posts.id
"""

pagingConditionsSQL = f"""
    AND ($3 is null OR last_created_utc <= $3) -- created_before $3
    AND ($4 is null OR last_created_utc >= $4) -- created_after $4
    AND ($5 is null OR             rate <= $5) -- rate_less $5
    AND ($6 is null OR             rate >= $6) -- rate_more $6
"""

commentFromJoinWhereSQL = f"""
{commentFromJoinSQL}
WHERE last_id IN
  (SELECT    last_id
      {commentFromJoinSQL}
      WHERE subreddit=$1
      {pagingConditionsSQL}
"""

postFromJoinSQL = """
FROM      aggregate_posts
LEFT JOIN posts
ON        aggregate_posts.id_of_max_pos_removed_item = posts.id
"""

postFromJoinWhereSQL = f"""
{postFromJoinSQL}
WHERE last_id IN
    (SELECT last_id
    {postFromJoinSQL}
    WHERE subreddit=$1
    {pagingConditionsSQL}
"""

endOfFunctionSQL = f"""
  USING subreddit, num_records, created_before, created_after, rate_less, rate_more;
END;
$$ language plpgsql STABLE;
"""

class Launcher():
    def __init__(self, dbConfigFile, mode='normal'):
        dbconfig = ConfigTyped(dbConfigFile, mode)
        dbopts = dbconfig.opts

        engine = create_engine(dbconfig.get_connectString(dbopts['db_name']), pool_pre_ping=True)
        if engine.dialect.has_schema(engine, dbopts['db_schema_tmp']):
            with engine.connect() as con:
                con.execute('COMMIT;')
                con.execute(
                    f"""
                        DO $$ DECLARE
                            r RECORD;
                        BEGIN
                            -- if the schema you operate on is not "current", you will want to
                            -- replace current_schema() in query with 'schematodeletetablesfrom'
                            -- *and* update the generate 'DROP...' accordingly.
                            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                                EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                            END LOOP;
                        END $$;
                    """
                )
                con.execute(
                    f"""
                        DO $$ DECLARE
                            r RECORD;
                        BEGIN
                            -- if the schema you operate on is not "current", you will want to
                            -- replace current_schema() in query with 'schematodeletetablesfrom'
                            -- *and* update the generate 'DROP...' accordingly.
                            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = '{dbopts['db_schema_tmp']}') LOOP
                                EXECUTE 'ALTER TABLE {dbopts['db_schema_tmp']}.' || quote_ident(r.tablename) || ' SET SCHEMA public';
                            END LOOP;
                        END $$;
                    """
                )
                con.execute(
                    f"""
                        DROP SCHEMA {dbopts['db_schema_tmp']} CASCADE;
                    """
                )
        engine = create_engine(dbconfig.get_connectString(dbopts['db_name']), pool_pre_ping=True)
        with engine.connect() as con:
            log('6-create-db-functions indices start')
            ## Not using these two foreign keys b/c aggregate_ tables may not have
            ## corresponding comments/posts entries for data that couldn't
            ## be downloaded from pushshift via the AddFields process.
            ## As a result, when pulling data, aggregate_ tables have some scores that don't appear
            ## in comments/posts, so scores should be pulled from aggregate_ table
            ## So, the score field copied to comments/posts is not used now, and left as-is in case needed later

            #ALTER TABLE aggregate_comments ADD CONSTRAINT fk_mpri_id_c FOREIGN KEY (id_of_max_pos_removed_item) REFERENCES comments (id);
            #ALTER TABLE aggregate_posts ADD CONSTRAINT fk_mpri_id_p FOREIGN KEY (id_of_max_pos_removed_item) REFERENCES posts (id);

            ## Similarly, b/c PS queries fail to download some posts data, this constraint isn't possible to include,

            # 'ALTER TABLE comments ADD CONSTRAINT fk_post_id FOREIGN KEY (link_id) REFERENCES posts (id);'
            for index in indexes:
                try:
                    con.execute(index)
                    log('Successful: '+index)
                except:
                    pass
                    #log('WARNING: Index creation failed (it may already exist)')
                    #log('         '+index)
            log('6-create-db-functions functions start')
            con.execute(f"""
DROP FUNCTION IF EXISTS getCommentUpvoteRemovedRatesByRate;
DROP FUNCTION IF EXISTS getCommentUpvoteRemovedRatesByDate;

DROP TABLE IF EXISTS commentColumnsReturnTypes;
CREATE TABLE commentColumnsReturnTypes ( {commentColumnsReturnTypes} );

CREATE OR REPLACE function
  getCommentUpvoteRemovedRatesByRate({functionParamSQL}) RETURNS SETOF commentColumnsReturnTypes
AS
$$
BEGIN
  RETURN query execute 'SELECT {commentColumnsSelectSQL}
    {commentFromJoinWhereSQL}
        ORDER BY rate DESC LIMIT $2)
    ORDER BY rate DESC'
{endOfFunctionSQL}

CREATE OR REPLACE function
  getCommentUpvoteRemovedRatesByDate({functionParamSQL}) RETURNS SETOF commentColumnsReturnTypes
AS
$$
BEGIN
  RETURN query execute 'SELECT {commentColumnsSelectSQL}
    {commentFromJoinWhereSQL}
        ORDER BY last_created_utc DESC LIMIT $2)
    ORDER BY last_created_utc DESC'
{endOfFunctionSQL}

DROP FUNCTION IF EXISTS getPostUpvoteRemovedRatesByRate;
DROP FUNCTION IF EXISTS getPostUpvoteRemovedRatesByDate;

DROP TABLE IF EXISTS postColumnsReturnTypes;
CREATE TABLE postColumnsReturnTypes ( {postColumnsReturnTypes} );

CREATE OR REPLACE function
  getPostUpvoteRemovedRatesByRate({functionParamSQL}) RETURNS SETOF postColumnsReturnTypes
AS
$$
BEGIN
  RETURN query execute 'SELECT {postColumnsSelectSQL}
    {postFromJoinWhereSQL}
        ORDER BY rate DESC LIMIT $2)
    ORDER BY rate DESC'
{endOfFunctionSQL}

CREATE OR REPLACE function
  getPostUpvoteRemovedRatesByDate({functionParamSQL}) RETURNS SETOF postColumnsReturnTypes
AS
$$
BEGIN
  RETURN query execute 'SELECT {postColumnsSelectSQL}
    {postFromJoinWhereSQL}
        ORDER BY last_created_utc DESC LIMIT $2)
    ORDER BY last_created_utc DESC'
{endOfFunctionSQL}

UPDATE pg_language SET lanvalidator = 2247 WHERE lanname = 'c';

""".replace('%','%%'))
        log('finished')

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description = 'Create database functions.')
    ap.add_argument('-m', '--mode', type=str, help="Run mode",
                    default='normal')
    ap.add_argument('-d', '--dbconfig', type=str, help="Database config file", default='dbconfig.ini')
    args = ap.parse_args()
    l = Launcher(args.dbconfig,
                 mode=args.mode)
