#!/usr/bin/env python
# encoding: utf-8

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
# This file is copied from
# https://github.com/apache/impala/blob/branch-2.9.0/common/function-registry/impala_functions.py
# and modified by Doris

import os
import re
import time
from typing import Tuple, List

DORIS_HOME = '../../../../'
cur_relative_path = ''
log_path = 'res.log'


def run_file(file_path: str):
    if os.path.isdir(file_path):
        return
    f = open(file_path, 'r')
    text = f.read()
    f.close()
    suite_names: List[str] = re.findall('suite\(\"[A-Za-z-_0-9]+\"\)', text)
    if len(suite_names) != 1:
        return
    suite_name: str = suite_names[0].replace('suite("', '').replace('")', '')
    print(suite_name)
    f = open(file_path, 'w')
    index = text.find('{\n') + 2
    f.write(text[:index] + "sql 'set enable_nereids_planner=true'\n"
            + "sql 'set enable_fallback_to_original_planner=false'\n" + text[index:])
    f.close()
    os.system(f'sh {DORIS_HOME}run-regression-test.sh --run -s {suite_name} > {DORIS_HOME}res.log')


def extractSql(log_str: str, index: int) -> str:
    find_uint = lambda s, sub: s.find(sub) & 0xffffffff
    log_str = log_str[index:]
    print(log_str)
    # we get the sql by find the next line of log(info/error/warn etc.)
    sql_end_index = min(find_uint(log_str, 'ERROR'), find_uint(log_str, 'WARN'), find_uint(log_str, 'INFO')) \
                    - len('\n2023-02-12 19:31:26.793 ')
    return log_str[:sql_end_index]


def check() -> Tuple[bool, str, str]:
    log_str = open(f'{DORIS_HOME}res.log').read()
    error_index = log_str.find('ERROR')
    if error_index == -1:
        return True, "", ""
    execute_index = log_str[:error_index].rfind('sql: ')
    error_sql = extractSql(log_str, execute_index + len('sql: '))
    error_str = log_str[error_index: error_index + log_str[error_index:].find('at ')]
    return False, error_sql, error_str


def adjustTest(file_path: str, error_sql: str):
    f = open(file_path, 'r')
    text = f.read()
    f.close()
    sql_index = text.find(error_sql)
    print(sql_index)
    prev_line_index = text[:sql_index].rfind('\n') + 1
    next_line_index = text[sql_index + len(error_sql):].find('\n') + sql_index + len(error_sql)
    print(prev_line_index, next_line_index)
    new_text = text[:prev_line_index] + "sql 'set enable_nereids_planner=false'\n" \
               + text[prev_line_index: next_line_index] + "\nsql 'set enable_nereids_planner=true'" \
               + text[next_line_index:]
    f = open(file_path, 'w')
    f.write(new_text)
    f.close()


def checkDBAliveAndRerun(port: str):
    time.sleep(5)
    log = os.popen(f'mysql -h:: -P{port} -uroot -e "show backends"').readlines()[1].split('\t')[9]
    if log != 'true':
        os.system(f'sh {DORIS_HOME}output/be/bin/start_be.sh --daemon')
    time.sleep(10)


log_f = open(log_path, 'a')


def runProcess(file_path_name: str):
    while True:
        run_file(file_path_name)
        status, sql, log = check()
        if status:
            break
        log_f.write(sql)
        log_f.write(log)
        adjustTest(file_path_name, sql)
        checkDBAliveAndRerun('6786')


def run(file_path: str):
    for file_name in os.listdir(file_path):
        file_path_name = os.path.join(file_path, file_name)
        print(file_path_name)
        if os.path.isdir(file_path_name):
            run(file_path_name)
        else:
            runProcess(file_path_name)


runProcess('../scalar_function/A.groovy')
