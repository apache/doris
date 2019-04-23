#!/usr/bin/env bash

source `dirname ${BASH_SOURCE[0]}`/../../conf/env

function check_insert_load_palo_func(){
    local palo=$1
    local sql=$2
    insert_return_info=`${palo} -e "${sql}" -vv`

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
        echo "${palo} -e show load where label = '${label}' order by createtime desc limit 1"
        result=`${palo} -e "show load where label = '${label}' order by createtime desc limit 1" -N`

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
