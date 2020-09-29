#!/bin/bash

dailyDir=0-pushshift_raw_daily
rawDir=0-pushshift_raw
cd data/$rawDir

ls *-*-*.gz 2>/dev/null | cut -d - -f1,2 | sort -u | while read month; do
  files=$(ls $month-* | sort | tr '\n' ' ')
  cat $files > $month.gz
  mv $files ../$dailyDir/
done
