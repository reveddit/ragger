#!/bin/bash

extension='.zst'
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

rawDir=0-pushshift_raw
dailyDir=0-pushshift_raw_daily
echoerr(){ >&2 echo "$@"; }

cd data/$rawDir
error=false
ls *-*-*$extension 2>/dev/null | cut -d - -f1,2 | sort -u | while read type_year_month; do
  outputFile=${type_year_month}${extension}
  if [[ ! -f "$outputFile" ]] ; then
    files=$(ls $type_year_month-* | sort | tr '\n' ' ')
    for file in $files; do
      if ! "$SCRIPT_DIR/verifyFileSize.sh" "$file"; then
        error=true
      fi
    done
  else
    echoerr ERROR: file already exists [$outputFile]
    exit 1
  fi
  if [[ "$error" == "false" ]] ; then
    cat $files > $outputFile
    mv $files ../$dailyDir/
  else
    exit 1
  fi
done
