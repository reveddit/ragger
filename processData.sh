#!/bin/bash

script=${1:-all} # can be [all, slim, aggM, aggA, addF]
mode=${2:-normal} # can be [normal,test, or whatever appears in config.ini]
waitUntilCommandFinishes=${3:-no}

logType=processData-$script-$mode

slim="python 1-pushshift-slim.py -m $mode"
aggM="python 2-aggregate-monthly.py -m $mode"
aggA="python 3-aggregate-all.py -m $mode"
addF="python 4-add-fields.py -m $mode"
command="$slim; $aggM; $aggA; $addF"

if [ $script == slim ] ; then
    command=$slim
elif [ $script == aggM ] ; then
    command=$aggM
elif [ $script == aggA ] ; then
    command=$aggA
elif [ $script == addF ] ; then
    command=$addF
elif [ $script == all ] ; then
    :
else
    echo "Unknown command: [$script]"
    exit 1
fi

logBase="log-$logType"
./keepLog.sh $logType "$command" $waitUntilCommandFinishes
