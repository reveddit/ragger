# ragger
Aggregate reddit data in Pushshift monthly files for display on revddit.com

## Basic usage

To process the **test** dataset included in this repo, run `./processData.sh all test`. Results appear in `test/3-aggregate_all` and `test/4-add_fields`.

To process **full** results,

1. Download pushshift monthly dumps
1. Store them in `data/0-pushshift_raw/` as specified in `config.ini`
1. Run `./processData.sh all normal`

## With database

Install postgresql locally. The file `dbconfig-example.ini` contains database credentials.

* `./test.sh` loads **test** results into the database.
* `./test.sh normal` loads **full** results into the database.

## With remote database using Hasura

1. Set up a Hasura instance on digital ocean following [this guide](https://docs.hasura.io/1.0/graphql/manual/guides/deployment/digital-ocean-one-click.html)
1. Set up postgresql database credentials
1. Set up ssh keys

Remotely on the DO droplet,

1. `sudo ufw allow 9090/tcp`
1. `git clone https://github.com/reveddit/ragger.git`
1. `cd ragger`
1. `cp dbconfig-example.ini dbconfig.ini` and set database credentials in the new file
1. Add the top 4 lines of `droplet-config/pg_hba.conf.head` to `/var/lib/docker/volumes/hasura_db_data/_data/pg_hba.conf`
1. `sudo cp droplet-config/Caddyfile droplet-config/docker-compose.yaml /etc/hasura/`
1. Set the droplet's domain name in `/etc/hasura/Caddyfile`
1. Set admin secret and postgresql password in `/etc/hasura/docker-compose.yaml`
1. `sudo docker-compose up -d`

Then, locally,

1. In `prod.sh` change `api.revddit.com` to the domain name of the droplet
1. Run `prod.sh`
1. Check the local and remote logs to know when it's done
