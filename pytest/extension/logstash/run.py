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

import env
import argparse
import pymysql
import subprocess
import time
import json
import datetime

def tuple_to_str(tuple):
    ret = ''
    for e in tuple:
        if isinstance(e, datetime.datetime):
            ret += e.strftime('%Y-%m-%d %H:%M:%S') + '\t'
        elif isinstance(e, str) and e[0] == '{' and e[-1] == '}':
            ret += json.dumps(json.loads(e), sort_keys=True) + '\t'
        else:
            ret += str(e) + '\t'
    return ret[:-1]

def run(args, first_run=False):
    case_name = args.case
    timeout = args.timeout
    workers = args.workers
    batch_size = args.batch_size
    batch_delay = args.batch_delay

    conf = f'{env.TEST_CONF_DIR}/{case_name}.conf'
    print('conf:', conf)
    ddl = f'{env.TEST_DDL_DIR}/{case_name}.sql'
    print('ddl:', ddl)
    input = f'{env.TEST_DATA_DIR}/{case_name}.input'
    print('input:', input)
    output = f'{env.TEST_DATA_DIR}/{case_name}.output'
    print('output:', output)

    db = pymysql.connect(**env.DB_CONFIG)
    cursor = db.cursor()
    cursor.execute(f"create database if not exists {env.DB};")
    cursor.execute(f"use {env.DB};")
    cursor.execute(f"drop table if exists {case_name};")

    with open(ddl, 'r') as f:
        ddl = f.read()
        cursor.execute(ddl)
    
    cmd = [
        f"{env.LOGSTASH_HOME}/bin/logstash", 
        "-f", conf,
        "-w", str(workers),
        "-b", str(batch_size),
        "-u", str(batch_delay)
        ]
    print(f'Running command: {cmd} < {input}')
    
    with open (input, 'r') as f:
        try:
            p = subprocess.Popen(cmd, stdin=f, text=True)
            time.sleep(10)
            if p.poll() is not None:
                raise Exception(f'Failed to run {cmd}')
            # wait for logstash to finish the load process
            time.sleep(timeout)
        finally:
            p.kill()
            p.wait()        

    sql = f"select * from {case_name};"
    cursor.execute(sql)
    result = cursor.fetchall()
    
    result_list = []
    for row in result:
        result_list.append(tuple_to_str(row))
    result_list.sort()

    if first_run:  # only for debug
        with open(output, 'w') as f:
            for row in result_list:
                f.write(f'{row}\n')
    
    correct_list = []
    with open(output, 'r') as f:
        for line in f:
            correct_str = line[:-1]
            correct_list.append(correct_str)
    correct_list.sort()

    if str(result_list) != str(correct_list):
        raise Exception(f"Incorrect result:\n{'\n'.join(result_list)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--case", type=str, help="case name: (also data_file_name, config_file_name, table_name)", default='csv_basic')
    parser.add_argument("-t", "--timeout", type=int, help="timeout", default=20)
    parser.add_argument("-w", "--workers", type=int, help="pipeline workers", default=1)
    parser.add_argument("-b", "--batch-size", type=int, help="pipeline batch size", default=100)
    parser.add_argument("-u", "--batch-delay", type=int, help="pipeline batch delay", default=3000)
    args = parser.parse_args()
    
    # run(args, True)
    run(args)
