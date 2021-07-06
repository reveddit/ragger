#!/bin/bash

mode=${1:-test} # can be [test, normal]
dbconfig=${2:-dbconfig-example.ini} # path to config file

command="python 5-load-db.py -m $mode -d $dbconfig && python 6-create-db-functions.py -m $mode -d $dbconfig"

logType=db-$mode
./keepLog.sh $logType "$command"
