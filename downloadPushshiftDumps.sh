#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

outputFolder="$SCRIPT_DIR/data/0-pushshift_raw/"

"$SCRIPT_DIR/createDirectories.sh"

./monthlyFileDownloader.sh C .bz2 2005-12 2017-11 "$outputFolder"
./monthlyFileDownloader.sh C  .xz 2017-12 2018-09 "$outputFolder"
./monthlyFileDownloader.sh C .zst 2018-10 2019-12 "$outputFolder"
./monthlyFileDownloader.sh S .bz2 2012-01 2014-12 "$outputFolder"
./monthlyFileDownloader.sh S .zst 2015-01 2016-12 "$outputFolder"
./monthlyFileDownloader.sh S .bz2 2017-01 2017-10 "$outputFolder"
./monthlyFileDownloader.sh S .xz  2017-11 2018-10 "$outputFolder"
./monthlyFileDownloader.sh S .zst 2018-11 2020-04 "$outputFolder"

./dailyFileDownloader.sh 2020-01-01 2020-06-30 "$outputFolder"
