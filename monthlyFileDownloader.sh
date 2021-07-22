#!/bin/bash

# Usage:
#   ./monthlyFileDownloader.sh C .bz2 2005-12 2017-11 data/0-pushshift_raw_daily/
#   See ./downloadPushshiftDumps.sh for more

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

C_OR_S="${1:-C}" #C or S for comments or submissions
extension="${2}" # file extension that matches what's available on files.pushshift.io
startDate="${3}" # date in the format 2020-01
endDate="${4}"  # date in the format 2020-01
outputDir="${5:-$SCRIPT_DIR}"

thingType='comments'
if [[ "$C_OR_S" == "S" ]] ; then
  thingType='submissions'
fi

dateFormat="+%Y-%m"
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

current=$(parseDate "${startDate}-01")
end=$(parseDate "${endDate}-01")
current_seconds=$(sinceEpoch "${current}-01")
end_seconds=$(sinceEpoch "${end}-01")

while (( "$current_seconds" <= "$end_seconds" ))
do
  current_year=$(date "+%Y" -d@${current_seconds})
  filename_root=R${C_OR_S}_
  filename=${filename_root}${current}${extension}
  filename_base_month=${filename_root}${current}
  filename_base_year=${filename_root}${current_year}
  localpath_base_month=$(ls "$outputDir/${filename_base_month}".* 2>/dev/null)
  localpath_base_year=$(ls "$outputDir/${filename_base_year}".* 2>/dev/null)
  url="https://files.pushshift.io/reddit/$thingType/$filename"
  ## Uncomment to report error if remote file does not exist
  # if ! curl --head --fail --silent "$url" >/dev/null; then
  #   echo DOES NOT EXIST: $url
  # fi
  if [[ -z "$localpath_base_year" && -z "$localpath_base_month" ]] ; then
    wget --continue --directory-prefix="$outputDir" https://files.pushshift.io/reddit/$thingType/$filename
  else
    ls -l $localpath_base_month $localpath_base_year 2>/dev/null
  fi
  current=$(parseDate "${current}-01 + 1 month")
  current_seconds=$(sinceEpoch "${current}-01")
done
