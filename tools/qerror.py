#!/usr/bin/env python
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

import os
import subprocess

import requests
import json
import time

# Change the host port username password and database name on your need
mycli_cmd = "mysql -h127.0.0.1 -P9030 -uroot -Dtpch1G"

# FE http://host:port
feHttp = "http://localhost:8030"
trace_url = feHttp + '/rest/v2/manager/query/trace_id/{}'
qerror_url = feHttp + '/rest/v2/manager/query/qerror/{}'

# File path to save test results.
# Sample:
# 8
# {
#   "legacyPlanIdToPhysicalPlan": {
#     "0": {
#       "first": 1.0,
#       "second": 1.0
#     },
#    .......
#   "qError": 34.5
# }
# `8` represents q8 in the tpc-h test
# `first` is the estimated row count for plan which with plan id 0, `second` is the actual returned row count
qerr_saved_file_path = ""

# SQL under this directory would be tested.
original_sql_dir = "add your tpc-h/tpch-ds/ssb sql directory path here"

sql_file_prefix_for_trace = """
    SET enable_nereids_planner=true;
    SET session_context='trace_id:{}';
"""

q_err_list = []


def extract_number(string):
    return int(''.join([c for c in string if c.isdigit()]))


def write_results(path: str, title: str, result: list):
    with open(path, "a") as file:
        file.write(title)
        file.write("\n")
        for item in result:
            file.write(str(item) + " " + "\n")
        file.write("\n")


def read_lines(path: str) -> list:
    with open(path, "r") as f:
        return f.readlines()


def write_result(title: str, result: str):
    wrapped = [result]
    write_results(qerr_saved_file_path, title, wrapped)


def execute_command(cmd: str):
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout


def execute_sql(sql_file: str):
    command = mycli_cmd + " < " + sql_file
    result = execute_command(command).decode("utf-8")
    return result


def get_q_error(trace_id):
    time.sleep(1)
    # 'YWRtaW46' is the base64 encoded result for 'admin:'
    headers = {'Authorization': 'BASIC YWRtaW46'}
    resp_wrapper = requests.get(trace_url.format(trace_id), headers=headers)
    resp_text = resp_wrapper.text
    query_id = json.loads(resp_text)["data"]
    resp_wrapper = requests.get(qerror_url.format(query_id), headers=headers)
    resp_text = resp_wrapper.text
    write_result(str(trace_id), resp_text)
    print(trace_id)
    print(resp_text)
    qerr = json.loads(resp_text)["qError"]
    q_err_list.append(float(qerr))


def iterates_sqls(path: str, if_write_results: bool) -> list:
    cost_times = []
    files = os.listdir(path)
    files.sort(key=extract_number)
    for filename in files:
        if filename.endswith(".sql"):
            filepath = os.path.join(path, filename)
            traced_sql_file = filepath + ".traced"
            content = read_lines(filepath)
            sql_num = extract_number(filename)
            print("sql num" + str(sql_num))
            if if_write_results:
                write_results(traced_sql_file, str(sql_file_prefix_for_trace.format(sql_num)), content)
                execute_sql(traced_sql_file)
                get_q_error(sql_num)
                os.remove(traced_sql_file)
            else:
                execute_sql(filepath)
    return cost_times


if __name__ == '__main__':
    execute_command("echo 'set global enable_nereids_planner=true' | mysql -h127.0.0.1 -P9030")
    execute_command("echo 'set global enable_fallback_to_original_planner=false' | mysql -h127.0.0.1 -P9030")
    print("Preparing")
    iterates_sqls(original_sql_dir, False)
    print("Started...")
    iterates_sqls(original_sql_dir, True)
    write_results(qerr_saved_file_path, "AVG\n", [sum(q_err_list) / len(q_err_list)])
