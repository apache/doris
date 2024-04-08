#!/bin/bash
#
#
check_first_fe_status()
{
    local start_time=`date +%s`
    local expire_timeout=120
    local helper=$1
    while true; do
        output=`timeout 15 mysql --connect-timeout 2 -h $helper -P $FE_QUERY_PORT -u root --skip-column-names --batch -e "SHOW FRONTENDS;"`
    if [[ "x$output" != "x" ]]; then
        return 0
    fi

    let "expire=start_time+expire_timeout"
    local now=`date +%s`
    if [[ $expire -le $now ]]; then
        echo "[`date`] the first container is not started" >& 2
        exit 1
    fi

    sleep 2
    done
}

check_first_fe_status $1

