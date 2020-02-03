#!/bin/bash

mode=test
dbconfig=${1:-dbconfig-example.ini}

command="./processData.sh all $mode && ./loadDB.sh $mode $dbconfig"

logType=test
./keepLog.sh $logType "$command"
