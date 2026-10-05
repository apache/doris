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

# -*- coding: utf-8 -*-
import ConfigParser
import re
import sys
import os
import json
from urllib import urlopen

config = ConfigParser.ConfigParser()
config.read("conf")
username = config.get("cluster", "username")
passwd = config.get("cluster", "password")
fe_host = config.get("cluster", "fe_host")
fe_http_port = config.get("cluster", "http_port")
db=sys.argv[1]
table_name=sys.argv[2]

tbls=[]
f_table=open(table_name, "r")
line=f_table.readline()
while line:
    tbls.append(line)
    line=f_table.readline()
f_table.close()
fw_create=open("create.sql", "w")

for tbl in tbls:
    url_list=[]
    url_list.append("http://")
    url_list.append(username)
    url_list.append(":")
    url_list.append(passwd)
    url_list.append("@")
    url_list.append(fe_host)
    url_list.append(":")
    url_list.append(fe_http_port)
    url_list.append("/api/_get_ddl?db=default_cluster:")
    url_list.append(db)
    url_list.append("&tbl=")
    url_list.append(tbl)
    url = "".join(url_list)

    doc = urlopen(url).read();
    doc = json.loads(doc)

    create_table_stmt = doc["TABLE"]
    ddl = create_table_stmt[0].encode("utf-8")
    print ddl

fw_create.close()
