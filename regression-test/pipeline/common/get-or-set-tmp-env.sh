#!/bin/bash

tmp_env_file_path="${PWD}/.my_tmp_env"

usage() {
    echo -e "
Usage:
    $0 'get'
    $0 'set' \"export skip_pipeline='true'\"
    note: 'get' will return env file path; 'set' will add your new item into env file"
    exit 1
}

if [[ $1 == 'get' ]]; then
    if [[ ! -f "${tmp_env_file_path}" ]]; then touch "${tmp_env_file_path}"; fi
    echo "${tmp_env_file_path}"
elif [[ $1 == 'set' ]]; then
    if [[ -z $2 ]]; then usage; fi
    echo "$2" >>"${tmp_env_file_path}"
else
    usage
fi
