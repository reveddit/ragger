#!/bin/bash

logType=$1
command=$2
dateFormat="+%Y-%m-%d %H:%M:%S"
logBase="log-$logType"
logsRootDir=logs
logDir=$logsRootDir/$logType
mkdir -p $logDir
linkName="$logBase.txt"
link="$logsRootDir/$linkName"
find $logsRootDir -name $linkName -type l -exec rm {} \;

logFileName=log-$logType.$(date +"%Y-%m-%d_%H:%M:%S").txt
logFile=$logDir/$logFileName
logDirRelativeToLink=$logType
echo "tail -f $link"
echo "$(date '+%Y-%m-%d %H:%M:%S') start [$command]" > $logFile
ln -s $logDirRelativeToLink/$logFileName $link
nohup bash -c "$command && eval 'date \"$dateFormat\"' | tr '\n' ' ' && echo $logType finished" >> $logFile 2>&1 &
