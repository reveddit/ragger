#!/bin/bash

ARG1=${1:-all} # can be [all, slim, aggM, aggA, addF]
ARG2=${2:-normal} # can be [normal,test, or whatever appears in config.ini]

logType=processData-$ARG1-$ARG2

slim="python 1-pushshift-slim.py -m $ARG2"
aggM="python 2-aggregate-monthly.py -m $ARG2"
aggA="python 3-aggregate-all.py -m $ARG2"
addF="python 4-add-fields.py -m $ARG2"
command="$slim; $aggM; $aggA; $addF"

if [ $ARG1 == slim ] ; then
    command=$slim
elif [ $ARG1 == aggM ] ; then
    command=$aggM
elif [ $ARG1 == aggA ] ; then
    command=$aggA
elif [ $ARG1 == addF ] ; then
    command=$addF
elif [ $ARG1 == all ] ; then
    :
else
    echo "Unknown command: [$ARG1]"
    exit 1
fi

logBase="log-$logType"
./keepLog.sh $logType "$command"
