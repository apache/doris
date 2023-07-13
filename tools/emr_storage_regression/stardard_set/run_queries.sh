#!/bin/bash

set -e

FE_HOST=$1
USER=$2
FE_QUERY_PORT=$3
DB=$4
echo $DB

TRIES=3
QUERY_NUM=1
RESULT_FILE=result-master-"${DB}".csv
touch $RESULT_FILE
truncate -s 0 $RESULT_FILE

while read -r query; do
    echo -n "query${QUERY_NUM}," | tee -a $RESULT_FILE
    for i in $(seq 1 $TRIES); do
        RES=$( mysql -vvv -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e "${query}" | perl -nle 'print $1 if /((\d+\.\d+)+ sec)/' || :)
        echo -n "$RES" | tee -a $RESULT_FILE
        [[ "$i" != "$TRIES" ]] && echo -n "," | tee -a $RESULT_FILE
    done
    echo "" | tee -a $RESULT_FILE

    QUERY_NUM=$((QUERY_NUM + 1))
done < $5
