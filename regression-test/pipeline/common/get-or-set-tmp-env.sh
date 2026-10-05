#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
