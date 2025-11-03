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

import argparse
import sys
from typing import List
import json


class Record:
    def __init__(self, query_id, task_name, core_id, thread_id, start_time, end_time, group_id) -> None:
        self.query_id: str = query_id
        self.task_name: str = task_name
        self.core_id: int = int(core_id)
        self.thread_id: int = int(thread_id)
        self.start_time: int = int(start_time)
        self.end_time: int = int(end_time)
        self.group_id: int = int(group_id)

    def print(self):
        print(f"query_id = {self.query_id}, task_name = {self.task_name}, start_time={self.start_time}")

    def get_core(self):
        return 1 if same_core else self.core_id

    def to_json(self):
        json = {"name": self.task_name, "ph": "X", "ts": self.start_time, "dur": self.end_time - self.start_time,
                "pid": self.get_core(), "tid": self.thread_id, "args": {"tags": self.query_id}}
        return json


def get_key(record: Record) -> int:
    return record.start_time


def print_header(file):
    print(r'{"traceEvents":[', file=file)


def print_footer(file):
    print(r"]}", file=file)


parser = argparse.ArgumentParser(
    description='Accept file to analyse. Use parameters to sepecific how to illustrate it.')
parser.add_argument('-s', '--source', help='SOURCE as the path of tracing record file to analyze')
parser.add_argument('-d', '--dest', help='DEST as the path of json result file you want to save')
parser.add_argument('-n', '--no-core', action='store_true', help='combine the thread in one core group to display')
args = parser.parse_args()

records: List[Record] = []
same_core: bool = args.no_core

### get records
try:
    with open(args.source, "r") as file:
        for line in file:
            record = Record(*line.strip().split('|'))
            records.append(record)
except FileNotFoundError:
    sys.exit(f"File '{args.source}' doesn't exist. Please check the path.")
except Exception as e:
    sys.exit(f"Error occured! {e}")

records.sort(key=get_key)

### print json
try:
    with open(args.dest, "w") as file:  # overwrite file
        print_header(file)
        for record in records:
            print(json.dumps(record.to_json(), sort_keys=True, indent=4, separators=(',', ':')), end=',\n', file=file)
        print_footer(file)
    print(f"Generate json to {args.dest} succeed!")
except Exception as e:
    print(f"Error occured! {e}")
