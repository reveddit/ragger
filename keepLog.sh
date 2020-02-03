#!/bin/bash

logType=$1
command=$2

logBase="log-$logType"
link="$logBase.txt"
find . -maxdepth 1 -name $link -type l -exec trash {} \;
log=logs/log-$logType.$(date +"%Y-%m-%d_%H:%M").txt
mkdir -p logs
echo $command > $log
ln -s $log $link
nohup bash -c "$command" >> $log 2>&1 &
