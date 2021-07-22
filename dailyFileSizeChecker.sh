#!/bin/bash

# Usage: ./dailyFileDownloader.sh 2020-01-01 2020-06-30 data/0-pushshift_raw/

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

startDate="${1}" # any valid date
endDate="${2}"
filesDir="${3:-$SCRIPT_DIR/data/0-pushshift_raw}"


"$SCRIPT_DIR/createDirectories.sh"

fileSizesFile="$SCRIPT_DIR/data/remote_file_sizes.txt"

touch "$fileSizesFile"

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
  file=RC_$current.zst
  remoteFileSize=$(grep $file "$fileSizesFile" | awk '{print $2}')
  if [[ -z "$remoteFileSize" ]] ; then
    remoteFileSize=$(wget --spider --server-response -O - https://files.pushshift.io/reddit/comments/$file 2>&1 | sed -ne '/Content-Length/{s/.*: //;p}')
    echo $file $remoteFileSize >> "$fileSizesFile"
  fi
  localFileSize=$(stat -c %s $filesDir/$file)

  if [[ "$remoteFileSize" -eq "$localFileSize" ]] ; then
    echo $file size match
  else
    echo $file SIZE MISMATCH remote=$remoteFileSize
    echo $file SIZE MISMATCH  local=$localFileSize
  fi
  current=$(parseDate "$current + 1 day")
  current_seconds=$(sinceEpoch "$current")
done
