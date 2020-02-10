#!/bin/bash

mode=${1:-normal}
remote=${2:-api.revddit.com}
waitUntilCommandFinishes=${3:-no}

remoteRaggerDir=ragger
aggregate_all_dir=$(python getConfigVar.py -m $mode -v aggregate_all_dir)
add_fields_dir=$(python getConfigVar.py -m $mode -v add_fields_dir)

command="ssh $remote 'cd $remoteRaggerDir && mkdir -p $aggregate_all_dir $add_fields_dir' &&
scp $aggregate_all_dir/*.csv $remote:${remoteRaggerDir}/${aggregate_all_dir}/ &&
sleep 4 &&
scp $add_fields_dir/*.csv $remote:${remoteRaggerDir}/${add_fields_dir}/"

logType=scp
./keepLog.sh $logType "$command" $waitUntilCommandFinishes
