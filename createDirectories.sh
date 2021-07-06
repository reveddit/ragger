#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

root=$SCRIPT_DIR/data/

mkdir -p $root
cd $root

mkdir -p 0-pushshift_raw 0-pushshift_raw_daily 1-pushshift_slim 2-aggregate_monthly 3-aggregate_all 4-add_fields
