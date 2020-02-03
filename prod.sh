#!/bin/bash

mode=normal
dbconfig=dbconfig.ini

command="./processData.sh all $mode && ssh api.revddit.com 'cd ragger && ./loadDB.sh $mode $dbconfig'"

logType=prod
./keepLog.sh $logType "$command"
