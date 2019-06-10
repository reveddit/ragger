import argparse
import configparser
from sqlalchemy import create_engine
from logger import log
import sys


class Launcher():
    def __init__(self, configFile, mode='normal'):
        config = configparser.ConfigParser()
        config.read(configFile)
        opts = {}
        for g in ['default', mode]:
            for o in config.options(g):
                opts[o] = config.get(g, o)
                if opts[o].isdigit():
                    opts[o] = config.getint(g, o)
                elif opts[o].lower() in ['true', 'false']:
                    opts[o] = config.getboolean(g, o)
        log('6-create-db-functions')
        engine = create_engine('postgresql://'+
                                opts['db_user']+':'+
                                opts['db_pw']+'@localhost:'+
                                str(opts['db_port'])+'/'+
                                opts['db_name'])
        with engine.connect() as con:
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
    ap.add_argument('-c', '--config', type=str, help="Config file", default='config.ini')
    args = ap.parse_args()
    l = Launcher(args.config,
                 mode=args.mode)
