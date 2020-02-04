#!/bin/bash

executionLocation=/tmp/hasuraMetadataReload

secret=$(cat /etc/hasura/docker-compose.yaml | grep HASURA_GRAPHQL_ADMIN_SECRET | cut -d : -f2 | awk '{print $1}')

command="mkdir -p $executionLocation &&
cd $executionLocation &&
touch config.yaml &&
mkdir -p migrations &&
hasura metadata reload --endpoint https://api.revddit.com --admin-secret $secret"

logType=hasura
./keepLog.sh $logType "$command"
