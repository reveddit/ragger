#!/bin/bash

mode=${1:-test}
dbconfig=${2:-dbconfig-example.ini}

command="./processData.sh all $mode yes && ./loadDB.sh $mode $dbconfig"

logType=test-$mode
./keepLog.sh $logType "$command"
