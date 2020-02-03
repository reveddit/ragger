import argparse
import configparser
from sqlalchemy import create_engine
from logger import log
import sys

from ConfigTyped import ConfigTyped

class Launcher():
    def __init__(self, dbConfigFile, mode='normal'):
        dbconfig = ConfigTyped(dbConfigFile, mode)
        dbopts = dbconfig.opts

        engine = create_engine(dbconfig.get_connectString_for_user(dbopts['other_db_name']), pool_pre_ping=True)
        with engine.connect() as con:
            con.execute('COMMIT;')
            con.execute(
                f"DROP DATABASE IF EXISTS {dbopts['db_name']};"
            )
            con.execute('COMMIT;')
            con.execute(
                f"ALTER DATABASE {dbopts['db_tmp_name']} RENAME TO {dbopts['db_name']};"
            )
        engine = create_engine(dbconfig.get_connectString_for_user(dbopts['db_name']), pool_pre_ping=True)
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
            con.execute(
                'CREATE INDEX ix_subreddit_c ON aggregate_comments USING btree (subreddit);'
                'CREATE INDEX ix_subreddit_p ON aggregate_posts USING btree (subreddit);'

                'CREATE INDEX ix_utc_desc_c ON aggregate_comments USING btree (last_created_utc DESC NULLS LAST);'
                'CREATE INDEX ix_utc_desc_p ON aggregate_posts USING btree (last_created_utc DESC NULLS LAST);'

                'CREATE INDEX ix_rate_desc_c ON aggregate_comments USING btree (rate DESC NULLS LAST);'
                'CREATE INDEX ix_rate_desc_p ON aggregate_posts USING btree (rate DESC NULLS LAST);'

                'ALTER TABLE comments ADD PRIMARY KEY (id);'
                'ALTER TABLE posts ADD PRIMARY KEY (id);'
            )
            log('6-create-db-functions functions start')
            con.execute("""
DROP FUNCTION IF EXISTS getCommentUpvoteRemovedRatesByRate;
DROP FUNCTION IF EXISTS getCommentUpvoteRemovedRatesByDate;
DROP VIEW IF EXISTS commentRemovedRatesView;

CREATE OR REPLACE view commentRemovedRatesView AS
SELECT    subreddit,
          rate,
          last_created_utc,
          id_of_max_pos_removed_item,
          last_id,
          total_items,
          comments.body,
          comments.created_utc,
          posts.title,
          score_of_max_pos_removed_item AS score
FROM      aggregate_comments
LEFT JOIN comments
ON        aggregate_comments.id_of_max_pos_removed_item = comments.id
LEFT JOIN posts
ON        comments.link_id = posts.id;

CREATE OR REPLACE function
  getCommentUpvoteRemovedRatesByRate(subreddit VARCHAR(40)) RETURNS SETOF commentRemovedRatesView
AS
$$
BEGIN
  RETURN query execute format('SELECT * from commentRemovedRatesView WHERE subreddit=''%s'' ORDER BY rate DESC', subreddit);
END;
$$ language plpgsql STABLE;

CREATE OR REPLACE function
  getCommentUpvoteRemovedRatesByDate(subreddit VARCHAR(40)) RETURNS SETOF commentRemovedRatesView
AS
$$
BEGIN
  RETURN query execute format('SELECT * from commentRemovedRatesView WHERE subreddit=''%s'' ORDER BY last_created_utc DESC', subreddit);
END;
$$ language plpgsql STABLE;

DROP FUNCTION IF EXISTS getPostUpvoteRemovedRatesByRate;
DROP FUNCTION IF EXISTS getPostUpvoteRemovedRatesByDate;
DROP VIEW IF EXISTS postRemovedRatesView;

CREATE OR REPLACE view postRemovedRatesView AS
SELECT    subreddit,
          rate,
          last_created_utc,
          id_of_max_pos_removed_item,
          last_id,
          total_items,
          posts.title,
          posts.num_comments,
          posts.created_utc,
          score_of_max_pos_removed_item AS score
FROM      aggregate_posts
LEFT JOIN posts
ON        aggregate_posts.id_of_max_pos_removed_item = posts.id;

CREATE OR REPLACE function
  getPostUpvoteRemovedRatesByRate(subreddit VARCHAR(40)) RETURNS SETOF postRemovedRatesView
AS
$$
BEGIN
  RETURN query execute format('SELECT * from postRemovedRatesView WHERE subreddit=''%s'' ORDER BY rate DESC', subreddit);
END;
$$ language plpgsql STABLE;

CREATE OR REPLACE function
  getPostUpvoteRemovedRatesByDate(subreddit VARCHAR(40)) RETURNS SETOF postRemovedRatesView
AS
$$
BEGIN
  RETURN query execute format('SELECT * from postRemovedRatesView WHERE subreddit=''%s'' ORDER BY last_created_utc DESC', subreddit);
END;
$$ language plpgsql STABLE;


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
