#!/usr/bin/env python
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
