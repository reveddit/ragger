#!/bin/bash

executionLocation=/tmp/hasuraMetadata

migrationsDir=$executionLocation/migrations

secret=$(cat /etc/hasura/docker-compose.yaml | grep HASURA_GRAPHQL_ADMIN_SECRET | cut -d : -f2 | awk '{print $1}')

command="mkdir -p $migrationsDir &&
cp hasura-metadata.json $migrationsDir/metadata.json &&
cd $executionLocation &&
touch config.yaml &&
hasura metadata apply --endpoint https://api.revddit.com --admin-secret $secret"

logType=hasura
./keepLog.sh $logType "$command"
