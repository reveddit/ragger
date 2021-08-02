#!/bin/bash

# Usage: ./dailyFileDownloader.sh C 2020-01-01 2020-06-30 data/0-pushshift_raw/

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

C_OR_S="${1:-C}" #C or S for comments or submissions
startDate="${2}" # date in the format 2020-01 for monthly, or 2020-01-01 for daily
endDate="${3}" # date matching the format of startDate
filesDir="${4:-$SCRIPT_DIR/data/0-pushshift_raw}"
extension="${5:-.zst}"
validDateSuffix=""

getDatePeriod() {
  delimitersInString="${1//[0-9]}"
  delimiterCount="${#delimitersInString}"
  if [[ $delimiterCount -ge 1 && $delimiterCount -le 2 ]] ; then
    if [[ $delimiterCount -eq 1 ]] ; then
      printf month
    else
      printf day
    fi
  else
    echo ERROR: bad format [$1]
    exit 1
  fi
}
echoerr(){ >&2 echo $@; }

period=$(getDatePeriod "$startDate")
endPeriodType=$(getDatePeriod "$endDate")

if [[ "$period" != "$endPeriodType" ]] ; then
  echoerr ERROR: start and end period types do not match
  exit 1
fi

thingType='comments'
if [[ "$C_OR_S" == "S" ]] ; then
  thingType='submissions'
fi

"$SCRIPT_DIR/createDirectories.sh"

fileSizesFile="$SCRIPT_DIR/remote_file_sizes.txt"

if [[ ! -f "$fileSizesFile" ]] ; then
  touch "$fileSizesFile"
fi

dateFormat="+%Y-%m-%d"
if [[ "$period" == month ]] ; then
  dateFormat="+%Y-%m"
  validDateSuffix="-01"
fi

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


current=$(parseDate "$startDate")
end=$(parseDate "$endDate")
current_seconds=$(sinceEpoch "$current")
end_seconds=$(sinceEpoch "$end")
dailyFileMatch='.*-.*-'
error=false
while (( "$current_seconds" <= "$end_seconds" ))
do
  file=R${C_OR_S}_${current}${extension}
  remoteFileSize=$(grep $file "$fileSizesFile" | awk '{print $2}')
  if [[ -z "$remoteFileSize" ]] ; then
    remoteFileSize=$(wget --spider --server-response -O - https://files.pushshift.io/reddit/${thingType}/$file 2>&1 | sed -ne '/Content-Length/{s/.*: //;p}')
    if [[ ! -z "$remoteFileSize" ]] ; then
      if [[ "$remoteFileSize" =~ ^[0-9]+$ && $remoteFileSize -gt 1000 ]] ; then
        echo $file $remoteFileSize >> "$fileSizesFile"
        monthly=$(egrep -v "$dailyFileMatch" "$fileSizesFile" | grep . | sort -t _ -k2)
        daily=$(egrep "$dailyFileMatch" "$fileSizesFile" | sort)
        printf "$monthly\n$daily\n" > "$fileSizesFile"
      else
        echoerr ERROR: $file [$remoteFileSize] "(bad file size)"
        error=true
      fi
    else
      echoerr ERROR: remote file does not exist: [$file]
      error=true
    fi
  fi
  if [[ "$error" == "false" && ! -z "$remoteFileSize" ]] ; then
    echo $file $remoteFileSize
  fi
  current=$(parseDate "$current" "+ 1 $period")
  current_seconds=$(sinceEpoch "$current")

  ## TODO: where should this code go?
  ##       should be able to download all file sizes before downloading the files
  ##
    # localFile="$filesDir/$file"
    # if [[ -f "$localFile" ]] ; then
    #   localFileSize=$(stat -c %s $filesDir/$file)
    #   if [[ "$remoteFileSize" -eq "$localFileSize" ]] ; then
    #     echo $file size match
    #   else
    #     echo "$file SIZE MISMATCH remote=$remoteFileSize"
    #     echo "$file SIZE MISMATCH  local=$localFileSize"
    #   fi
    # fi
done


if [[ "$error" == "true" ]] ; then
  exit 1
fi
