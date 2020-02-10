#!/bin/bash

logType=$1
command=$2
waitUntilCommandFinishes=${3:-no}
date="%Y-%m-%d"
time="%H:%M:%S"
dateFormat="+$date $time"
dateFormatNoSpace="+${date}_${time}"
logBase="log-$logType"
logsRootDir=logs
logDir=$logsRootDir/$logType
mkdir -p $logDir
linkName="$logBase.txt"
link="$logsRootDir/$linkName"
find $logsRootDir -name $linkName -type l -exec rm {} \;

logFileName=log-$logType.$(date "$dateFormatNoSpace").txt
logFile=$logDir/$logFileName
logDirRelativeToLink=$logType
now="$(date "$dateFormat")"
echo "$now tail -f $link"
echo "$now start [$command]" > $logFile
ln -s $logDirRelativeToLink/$logFileName $link

commandWithDate="$command && eval 'date \"$dateFormat\"' | tr '\n' ' ' && echo $logType finished"

if [ "$waitUntilCommandFinishes" == 'no' ] ; then
  nohup bash -c "$commandWithDate" >> $logFile 2>&1 &
else
  bash -c "$commandWithDate" >> $logFile 2>&1
fi
