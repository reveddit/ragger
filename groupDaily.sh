#!/bin/bash

extension='zst'

dailyDir=0-pushshift_raw_daily
rawDir=0-pushshift_raw

cd data/$rawDir

ls *-*-*.$extension 2>/dev/null | cut -d - -f1,2 | sort -u | while read type_year_month; do
  files=$(ls $type_year_month-* | sort | tr '\n' ' ')
  outputFile=$type_year_month.$extension
  cat $files > $outputFile
  mv $files ../$dailyDir/
done
