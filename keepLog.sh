#!/bin/bash

logType=$1
command=$2
dateFormat="+%Y-%m-%d %H:%M:%S"
logBase="log-$logType"
link="$logBase.txt"
find . -maxdepth 1 -name $link -type l -exec trash {} \;
log=logs/log-$logType.$(date +"%Y-%m-%d_%H:%M").txt
mkdir -p logs
echo "$(date '+%Y-%m-%d %H:%M:%S') start [$command]" > $log
ln -s $log $link
nohup bash -c "$command && eval 'date \"$dateFormat\"' | tr '\n' ' ' && echo $logType finished" >> $log 2>&1 &
