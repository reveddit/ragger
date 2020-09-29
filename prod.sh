#!/bin/bash

mode=normal
dbconfig=dbconfig.ini
remote=ssh.rviewit.com

command="./groupDaily.sh && ./processData.sh all $mode yes && ./copyToRemote.sh $mode $remote yes && ssh $remote 'cd ragger && ./loadDB.sh $mode $dbconfig'"

logType=prod
./keepLog.sh $logType "$command"
