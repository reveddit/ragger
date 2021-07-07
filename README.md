# ragger
Aggregate reddit data in Pushshift monthly files for display on [***Rev***eddit.com/r/\<subreddit\>/top](https://www.reveddit.com/v/?contentType=top)

## Requirements

To process the full data set you need,

* 2TB HD: 1 TB of disk space to download the data and another 400 GB for intermediate processing files
* 40GB RAM: For the `2-aggregate-monthly.py` step. Splitting monthly files into smaller parts may use less memory.

Without this, you can run the code on the included test set in under a minute.

### Environment

Create a `conda` virtual environment and activate it,

```
conda create --name reveddit --file requirements-conda.txt
conda activate reveddit
```

Optionally, install PostgreSQL and include credentials in a `dbconfig.ini` as shown in `dbconfig-example.ini`

### Test

To process the **test** dataset included in this repo,

`./processData.sh all test`

Results appear in `test/3-aggregate_all` and `test/4-add_fields`.

To load results into a database, prepare database credentials in `dbconfig-example.ini` and run either,

* `./test.sh` runs the above command and load results into a local PostgreSQL database, or
* `./test.sh normal` loads **full** results into the database if files have been downloaded (see below)

### Download

To download all of the Pushshift dumps for both comments and submissions, run

```
./downloadPushshiftDumps.sh
```

The results will be in `data/0-pushshift_raw/`.

`./groupDaily.sh` creates monthly files from daily files and moves the daily files to another directory.

## Usage

To process **full** results,

1. Download pushshift monthly dumps
1. Store them in `data/0-pushshift_raw/` as specified in `config.ini`
1. Run `./processData.sh all normal`


## With a remote database

I used a DO droplet. These are the rough steps,

1. Set up ssh keys
1. Install Postgres with docker
1. Create a database login and password for your script
1. Add the top 4 lines of `droplet-config/pg_hba.conf.head` to `/var/lib/docker/volumes/hasura_db_data/_data/pg_hba.conf`
1. `sudo docker-compose up -d`
1. git clone this repo
1. Put the database login and password into a file called `dbconfig.ini` in the root directory of this repo

Then, locally,

1. In `prod.sh` change `ssh.rviewit.com` to the domain name of the droplet
1. Run `prod.sh`
1. Check the local and remote logs to know when it's done
