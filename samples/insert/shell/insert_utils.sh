#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


function check_insert_load_doris_func(){
    local doris=$1
    local sql=$2
    insert_return_info=`${doris} -e "${sql}" -vv`

    label=`echo ${insert_return_info}|perl -e '$_ = <>;chomp;/(\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12})/;print $1'`
    echo "${label}"
    wait_seconds=3600
    while [[ "${wait_seconds}" > 0 && "${label}" == '' ]];do
        label=`echo "${insert_return_info}"|perl -e '$_ = <>;chomp;/(\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12})/;print $1'`
        sleep 5s
        ((wait_seconds=${wait_seconds}-5))
    done

    if [[ "$label" == '' ]];then
        return 1
    fi

    wait_seconds=3600
    while [[ "${wait_seconds}" > 0 ]];do
        echo "${wait_seconds}"
        echo "${doris} -e show load where label = '${label}' order by createtime desc limit 1"
        result=`${doris} -e "show load where label = '${label}' order by createtime desc limit 1" -N`

        load_status=`echo "${result}"|perl -e '$_ = <>;chomp;/(\d+)\s(\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12})\s(\w+)\s/;print $3'`
        if [[ "${load_status}" == 'FINISHED' || "${load_status}" == 'CANCELLED' ]]; then
            echo "insert status: ${load_status}"
            if [[ "${load_status}" == 'FINISHED' ]];then
                return 0
            else
                return 1
            fi
            break
        else
            echo "insert status: ${load_status}"
            sleep 5s
            ((wait_seconds=$wait_seconds-5))
        fi
    done

    return 1
}

# check_insert_load.sh demo.
# You need input your doris db connect config & insert sql.
doris_host="127.0.0.1"
doris_port=8080
doris_user="db_user"
doris_password="db_password"
doris_database="db_name"
doris="mysql -h$doris_host -P$doris_port -u$doris_user -p$doris_password -D$doris_database"
sql="INSERT INTO TABLE_Y[(column1, column2,...,columnN)] SELECT column1, column2,..., columnN [FROM TABLE_X WHERE xxx]"
check_insert_load_doris_func ``"$doris"` `"$sql"
