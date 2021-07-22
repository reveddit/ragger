#!/bin/bash

# This script downloads a subset of Pushshift data for use with this project.
#       Comments: 2005/12 - 2007/10
#                 2015/09 - 2020/06
#    Submissions: 2018/06 - 2020/04
# 
# The data for comments between 2007/11 - 2015/08 is not usable for this project.
# Per check_comment_removed_counts.sh, no removed comments were recorded.
# Presumably, Pushshift did not store entries for removed comments for that period.
# You would need to redownload this period's comments from reddit to use them in this project.
# This download could be made faster by skipping any comment IDs present in the Pushshift dumps.
# It may or may not be useful for this project.  If the Pushshift API has the body text for these comments then it is useful.
# My guess is Pushshift does not have them since its collection process began later, as I understand it.
#
# Regarding submissions prior to 2018/06:
#    Some of Pushshift's record for this period is not usable.
#    It either does not have one of the fields needed for this project:
#         - is_crosspostable (old way of determining is_removed)
#         - is_robot_indexable (new way of determining is_removed)
#    Or, while is_crosspostable does exist in some data, it is not a proxy for is_removed (see check_post_removed_counts.sh)
#    Where the fields do not exist, they were added to the reddit API after Pushshift retrieved them.
#
#    For 2005/06 - 2010/12:
#       Pushshift's RS_v2_xx.xz files do contain is_crosspostable, however,
#       I believe is_crosspostable is not a proxy for is_removed for that period.
#       You would need to redownload this period's submissions from reddit to see is_robot_indexable
#    For 2011/01 - 2018/05:
#       You would need to redownload this period's submissions from reddit to see is_robot_indexable
# 
# A similar script for downloading Pushshift data exists here: https://github.com/guywuolletjr/reddit_content_mod/blob/master/pushshift/pull_data.py

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

outputFolder="$SCRIPT_DIR/data/0-pushshift_raw/"

"$SCRIPT_DIR/createDirectories.sh"

## Some of this period's Pushshift comment data is not usable by this project. See note above
  # ./monthlyFileDownloader.sh C .bz2 2005-12 2017-11 "$outputFolder"

./monthlyFileDownloader.sh C .bz2 2005-12 2007-10 "$outputFolder"
./monthlyFileDownloader.sh C .bz2 2015-09 2017-11 "$outputFolder"

./monthlyFileDownloader.sh C  .xz 2017-12 2018-09 "$outputFolder"
./monthlyFileDownloader.sh C .zst 2018-10 2019-12 "$outputFolder"
./dailyFileDownloader.sh 2020-01-01 2020-10-31 "$outputFolder" # get data in smaller parts where possible: easier to download
./monthlyFileDownloader.sh C .zst 2020-11 2020-12 "$outputFolder"

## These dates' Pushshift submission data are not usable by this project. See note above.
  #./monthlyFileDownloader.sh S .bz2 2012-01 2014-12 "$outputFolder"
  #./monthlyFileDownloader.sh S .zst 2015-01 2016-12 "$outputFolder"
  #./monthlyFileDownloader.sh S .bz2 2017-01 2017-10 "$outputFolder"
  #./monthlyFileDownloader.sh S .xz  2017-11 2018-10 "$outputFolder"

./monthlyFileDownloader.sh S .xz  2018-06 2018-10 "$outputFolder"
./monthlyFileDownloader.sh S .zst 2018-11 2020-04 "$outputFolder"


