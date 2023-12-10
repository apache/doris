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

export ROOT_PATH=$(pwd)
export SYS_PATH=${ROOT_PATH}/sys
export QE_PATH=${ROOT_PATH}/qe/palo2/src
export REPORT_PATH=${ROOT_PATH}/result

if [[ ! -d ${REPORT_PATH} ]]; then
    mkdir -p "${REPORT_PATH}"
fi

if [[ -z ${testsuite} ]]; then
    testsuite=normal
fi

function pytest_execute()
{
    case_file=$1
    ls "${case_file}"
    pytest -sv --junit-xml="${REPORT_PATH}"/"${case_file%.py}".xml --html="${REPORT_PATH}"/"${case_file%.py}".html  "${case_file}" --tb=native 2>&1 | tee "${case_file%.py}".log
    sleep 1
}

#一些客户场景case，单独执行，不并发
case='test_bitmap_2kw.py'
cd "${SYS_PATH}"/test_scene
pytest_execute "${case}"

cases='test_sys_update_restart.py test_sys_partition_complex_with_restart_be.py test_sys_restart.py test_sys_binlog_restart.py test_sys_resource_tag.py'
for i in ${cases}
do
    cd "${SYS_PATH}"
    echo "${i}"
    pytest_execute "${i}"
done

#所有case执行完后，检查集群的be是否有core
i=test_sys_check_core.py
cd "${SYS_PATH}"
echo "${i}"
pytest_execute "${i}"

echo 'FINISHED'
echo 'byebye'
