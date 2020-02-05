#!/bin/bash

mode=${1:-test} # can be [test, normal]
dbconfig=${2:-dbconfig-example.ini} # path to config file

baseCommand="python 5-load-db.py -m $mode -d $dbconfig && python 6-create-db-functions.py -m $mode -d $dbconfig"
commandSuffix=""

if [ $mode == 'normal' ] ; then
  commandSuffix="&& ./hasuraMetadataApply.sh"
fi

command="$baseCommand $commandSuffix"

logType=db-$mode
./keepLog.sh $logType "$command"
