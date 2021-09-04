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
validDateSuffix="-01"
function parseDate() {
    date="${1}${validDateSuffix}"
    add="${2}"
    dateToParse="${date} ${add}"
    if [[ "$var" =~ ^[0-9]+$ ]] ; then
      date "$dateFormat" -d@"$dateToParse";
    else
      date "$dateFormat" -d "$dateToParse";
    fi
};

function sinceEpoch() {
  dateToParse="${1}${validDateSuffix}"
  date -d "$dateToParse" +%s
}

current=$(parseDate "${startDate}")
end=$(parseDate "${endDate}")
current_seconds=$(sinceEpoch "${current}")
end_seconds=$(sinceEpoch "${end}")

while (( "$current_seconds" <= "$end_seconds" ))
do
  current_year=$(date "+%Y" -d@${current_seconds})
  filename_root=R${C_OR_S}_
  filename=${filename_root}${current}${extension}
  filename_base_month=${filename_root}${current}
  filename_base_year=${filename_root}${current_year}
  localpath_base_year=$(ls "$outputDir/${filename_base_year}".* 2>/dev/null)
  url="https://files.pushshift.io/reddit/$thingType/$filename"
  localpath_base_month="$outputDir/${filename}"

  if [[ -z "$localpath_base_year" ]] ; then
    remoteFileSize=$("$SCRIPT_DIR/getRemoteFileSizes.sh" "$C_OR_S" "$current" "$current" "$extension" | awk '{print $2}')
    localFileSize=''
    if [[ -f "$localpath_base_month" ]] ; then
      localFileSize=$(stat -c %s "$localpath_base_month")
    fi
    if [[ "$remoteFileSize" && (! -f "$localpath_base_month" || "$remoteFileSize" -ne "$localFileSize") ]] ; then
      wget --continue --tries=0 --directory-prefix="$outputDir" https://files.pushshift.io/reddit/$thingType/$filename
    fi
  else
    ls $localpath_base_year 2>/dev/null
  fi
  current=$(parseDate "${current}" "+ 1 month")
  current_seconds=$(sinceEpoch "${current}")
done
