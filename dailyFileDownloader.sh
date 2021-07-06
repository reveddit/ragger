#!/bin/bash

# Usage: ./dailyFileDownloader.sh 2020-01-01 2020-06-30 data/0-pushshift_raw_daily/

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

startInput="${1}" # any valid date
endInput="${2}"
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

current=$(parseDate "$startInput")
end=$(parseDate "$endInput")
current_seconds=$(sinceEpoch "$current")
end_seconds=$(sinceEpoch "$end")

while (( "$current_seconds" <= "$end_seconds" ))
do
  wget --no-clobber --directory-prefix="$outputDir" https://files.pushshift.io/reddit/comments/RC_$current.zst
  current=$(parseDate "$current + 1 day")
  current_seconds=$(sinceEpoch "$current")
done
