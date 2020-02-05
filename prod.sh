#!/bin/bash

mode=normal
dbconfig=dbconfig.ini
remote=api.revddit.com

command="./processData.sh all $mode && ./copyToRemote.sh $mode $remote && ssh $remote 'cd ragger && ./loadDB.sh $mode $dbconfig'"

logType=prod
./keepLog.sh $logType "$command"
