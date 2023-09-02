# coding=utf-8

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

import requests
from requests.auth import HTTPBasicAuth
import base64

# This script is a python demo for doris stream load
#
# How to use:
# 1. create a table in doris with any mysql client
# CREATE TABLE `db0`.`t_user` (
#   `id` int,
#   `name` string
# )
# DUPLICATE KEY(`id`)
# DISTRIBUTED BY HASH(`id`) BUCKETS 1
# PROPERTIES (
#   "replication_num" = "1"
# );
#
# 2. change the Doris cluster, db, user config in this class
#
# 3. run this script, you should see the following output:
#
# 200 OK
# {
#     "TxnId": 14017,
#     "Label": "2486da70-94bb-47cc-a810-70791add2b8c",
#     "TwoPhaseCommit": "false",
#     "Status": "Success",
#     "Message": "OK",
#     "NumberTotalRows": 2,
#     "NumberLoadedRows": 2,
#     "NumberFilteredRows": 0,
#     "NumberUnselectedRows": 0,
#     "LoadBytes": 13,
#     "LoadTimeMs": 54,
#     "BeginTxnTimeMs": 1,
#     "StreamLoadPutTimeMs": 12,
#     "ReadDataTimeMs": 0,
#     "WriteDataTimeMs": 11,
#     "CommitAndPublishTimeMs": 28
# }
if __name__ == '__main__':
    database, table = 'db0', 't_user'
    username, password = 'root', ''
    url = 'http://127.0.0.1:8030/api/%s/%s/_stream_load' % (database, table)
    headers = {
        'Content-Type': 'text/plain; charset=UTF-8',
        # 'label': 'your_custom_label',
        'format': 'csv',
        "column_separator": ',',
        'Expect': '100-continue',
        # 'Authorization': 'Basic ' + base64.b64encode((username + ':' + password).encode('utf-8')).decode('ascii')
    }
    auth = HTTPBasicAuth(username, password)
    session = requests.sessions.Session()
    session.should_strip_auth = lambda old_url, new_url: False  # Don't strip auth

    data='1,Tom\n2,Jelly'

    resp = session.request(
        'PUT', url=url,
        data=data, # open('/path/to/your/data.csv', 'rb'),
        headers=headers, auth=auth
    )

    print(resp.status_code, resp.reason)
    print(resp.text)
