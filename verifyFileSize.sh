#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echoerr(){ >&2 echo "$@"; }

fullfile="${1}"

filename=$(basename -- "$fullfile")
type=${filename:1:1}
extension="${filename##*.}"
date=$(echo "${filename%.*}" | cut -d _ -f2)

localFileSize=$(stat -c %s "$fullfile")

remoteFileSize=$("$SCRIPT_DIR/getRemoteFileSizes.sh" "$type" "$date" "$date" ".$extension" | awk '{print $2}')
if [[ ! "$remoteFileSize" || "$remoteFileSize" -ne "$localFileSize" ]] ; then
  echoerr "ERROR: size mismatch [$filename]"
  echoerr "          remote = ${remoteFileSize}"
  echoerr "           local = ${localFileSize}"
  exit 1
fi
