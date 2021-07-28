#!/bin/bash

# Usage: ./dailyFileDownloader.sh 2020-01-01 2020-06-30 data/0-pushshift_raw/

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

startDate="${1}" # any valid date
endDate="${2}"
outputDir="${3:-$SCRIPT_DIR}"

dateFormat="+%Y-%m-%d"
function parseDate() {
    for var in "$@"
    do
      if [[ "$var" =~ ^[0-9]+$ ]] ; then
        date "$dateFormat" -d@"$var";
      else
        date "$dateFormat" -d "$var";
      fi
    done
};

function sinceEpoch() {
  for var in "$@"
  do
    date -d "$var" +%s
  done
}

current=$(parseDate "$startDate")
end=$(parseDate "$endDate")
current_seconds=$(sinceEpoch "$current")
end_seconds=$(sinceEpoch "$end")

while (( "$current_seconds" <= "$end_seconds" ))
do
  fileBase=RC_$current.zst
  localFile="$outputDir/$fileBase"
  remoteFileSize=$("$SCRIPT_DIR/fileSizeChecker.sh" "$current" "$current" "$outputDir" | awk '{print $2}')
  localFileSize=''
  if [[ -f "$localFile" ]] ; then
    localFileSize=$(stat -c %s "$localFile")
  fi
  if [[ "$remoteFileSize" && (! -f "$localFile" || "$remoteFileSize" -ne "$localFileSize") ]] ; then
    wget --continue --directory-prefix="$outputDir" https://files.pushshift.io/reddit/comments/$fileBase
  fi
  current=$(parseDate "$current + 1 day")
  current_seconds=$(sinceEpoch "$current")
done
