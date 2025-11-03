#!/usr/bin/env python3
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

import subprocess
import json
import requests
import os
import re
import argparse

FE_HOST = "http://127.0.0.1:8030"
MYSQL = "mysql -h:: -P9030 -u root "
DB = "tpcds_sf100"
WORK_DIR="./tmp/"


def execute_command(cmd: str):
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    return result.stdout

def sql(query):
    print("exec sql: " + query)
    cmd = MYSQL + " -D " + DB + " -e \"" + query + "\""
    result = execute_command(cmd)
    return result

def last_query_id():
    # 'YWRtaW46' is the base64 encoded result for 'admin:'
    headers = {'Authorization': 'BASIC YWRtaW46'}
    query_info_url = FE_HOST + '/rest/v2/manager/query/query_info'
    resp_wrapper = requests.get(query_info_url, headers=headers)
    resp_text = resp_wrapper.text
    qinfo = json.loads(resp_text)
    assert qinfo['msg'] == 'success'
    assert len(qinfo['data']['rows']) > 0
    return qinfo['data']['rows'][0][0]

def get_profile(query_id):
    profile_url = FE_HOST + "/rest/v2/manager/query/profile/json/" + query_id
    headers = {'Authorization': 'BASIC YWRtaW46'}
    resp_wrapper = requests.get(profile_url, headers=headers)
    resp_text = resp_wrapper.text

    prof = json.loads(resp_text)
    assert prof['msg'] == 'success', "query failed"
    data = prof['data']['profile'].replace("\\n", "")
    return data.replace("\\u003d", "=")

def get_fragments(obj): 
    return obj['children'][2]['children'][0]['children']


def get_children(obj):
    return obj['children']

class Fragment:
    def __init__(self, obj):
        self.name = obj['name']
        self.instances = []
        for child in get_children(obj):
            self.instances.append(Instance(child))
        # the merged instance. merge corresponding Node.rows
        self.merged = Instance(get_children(obj)[0])
        self.merged.clear()
        self.merge_instances()
        
        m=re.search("Instance ([\w|\d]+-[\w|\d]+) ", self.merged.name)
        assert m != None, "cannot find instance id: " + self.merged.name
        self.first_inst_id = m.group(1)
        

    def merge_instances(self):
        for inst in self.instances:
            self.merged.merge(inst)

    def register(self, node_map):
        self.merged.register(node_map)

class Instance:
    def __init__(self, obj):
        self.name = obj['name']
        assert(len(get_children(obj)) > 1)
        self.sender = Node(get_children(obj)[0])
        self.dst_id = self.sender.get_dst_id()

        self.root = Node(get_children(obj)[1])
        if (len(get_children(obj)) > 2):
            self.pipe = Node(get_children(obj)[2])

    def clear(self):
        self.root.clear()

    def merge(self, other):
        self.root.merge(other.root)

    def register(self, node_map):
        self.root.register(node_map)

class Node:
    def __init__(self, obj):
        self.obj = obj
        self.name = self.get_name(obj)
        if self.name == "RuntimeFilter":
            self.rf_id = self.get_rf_id(obj)
            self.id = -1
            self.caption = self.name + "_" + str(self.rf_id)
        else:
            self.id = self.get_id(obj)
            self.caption = self.name + "_" + str(self.id)

        self.rows = obj['rowsReturned']
        self.total_time = obj['totalTime']
        self.children = []
        for child in obj['children']:
            self.children.append(Node(child))

    def get_name(self, obj):
        name = obj['name']
        m=re.search("(\S+)", obj['name'])
        if m != None:
            name = m.group(1)
        return name.replace("(", '_').replace(")", '').replace(':', '')

    def get_id(self, obj):
        m=re.search("id=([\d]+)", self.obj['name'])
        if m != None:
            return int(m.group(1))
        else:
            return -1

    def get_rf_id(self, obj):
        m=re.search("id = ([\d]+)", self.obj['name'])
        if m != None:
            return int(m.group(1))
        else:
            return -1
        
    def get_dst_id(self):
        m=re.search("dst_id=([\d]+)", self.obj['name'])
        if m != None:
            return int(m.group(1))
        else:
            return -1
        
    def clear(self):
        self.rows = 0
        self.total_time = ""
        for child in self.children:
            child.clear()

    def register(self, node_map):
        node_map[self.id] = self
        for child in self.children:
            child.register(node_map)

    def merge(self, other):
        assert(self.name == other.name)
        self.rows += other.rows
        self.total_time += "/" + other.total_time
        childCount =len(self.children)
        if self.name.find("ScanNode") > 0:
            return
        assert(childCount == len(other.children))
        for i in range(childCount):
            self.children[i].merge(other.children[i])

    def tree(self, prefix):
        print(prefix + self.name + " id=" + str(self.id) + " rows=" + str(self.rows))
        for child in self.children:
            child.tree(prefix + "--")

    def to_dot(self):
        content = ""
        list = self.children
        if list != None:
            list.reverse()
        for child in list:
            if child.id != -1:
                content += "{}->{}[label={}]\n".format(child.caption, self.caption, child.rows)
        for child in list:
            if child.id != -1:
                content += child.to_dot()
        return content
            
def keep_alpha_numeric(input_str):  
    # 使用正则表达式只保留大写字母、小写字母和数字  
    pattern = r'[^A-Za-z0-9]'  
    result = re.sub(pattern, '', input_str)  
    return result 

def to_dot_file(root, dot_file, title):
    title = keep_alpha_numeric(title)
    header = """
            digraph BPMN {
                rankdir=BT;
                node[style="rounded,filled" color="lightgoldenrodyellow" ]
            """
    label = "label = \"" + title + "\"\n"
    tail = "}"
    with open(dot_file, 'w') as f:
        f.write(header)
        f.write(label)
        f.write(root.to_dot())
        f.write(tail)

def draw_proile(query_id, title=""):
    print("query_id: " + query_id)
    prof = get_profile(query_id)
    if title == "":
        title = query_id
    prf_file = WORK_DIR + title + ".profile"
    png_file = WORK_DIR + title + ".png"
    dot_file = WORK_DIR + title + ".dot"
    with open(prf_file, "w") as f:
        f.write(prof)
    obj = json.loads(prof.replace("\n", ""))
    fragments = []
    node_map={}
    for json_fr in get_fragments(obj):
        fr = Fragment(json_fr)
        fr.register(node_map)
        fragments.append(fr)
    root_fr = fragments[0]
    for fr in fragments[1:]:
        exchange = node_map[fr.merged.dst_id]
        exchange.children.append(fr.merged.root)
    
    root_fr.merged.root.tree("")
    
    to_dot_file(root_fr.merged.root, dot_file, title)
    cmd = "dot {} -T png -o {}".format(dot_file, png_file)
    print(cmd)
    status = os.system(cmd)
    if status != 0:
        print("convert to png failed")
    else:
        print("generate " + png_file)

def exec_sql_and_draw(sql_file):
    if not os.path.isdir(WORK_DIR):
        os.mkdir(WORK_DIR)
    with open(sql_file, 'r') as f:
        #trim sql comments
        query = ""
        for line in f.readlines():
            pos = line.find("--")
            if pos == -1:
                query += line
            else:
                query += line[:pos]
        sql("set global enable_nereids_planner=true")
        sql("set global enable_pipeline_engine=true")
        sql("set global enable_profile=true")
        sql("set global runtime_filter_type=2")
        sql("set global enable_runtime_filter_prune=false")
        sql("set global runtime_filter_wait_time_ms=9000000")
        sql(query)
        query_id = last_query_id()
        return query_id


def print_usage():
    print("""
    USAGE:
    1. execute a sql file, and draw its profile  
          python3 profile_viewer.py -f [path to sql file]
    2. draw a given profile
          python3 profile_viewer.py -qid [query_id]
    
    graphviz is required(https://graphviz.org/)
    on linux: apt install graphviz
    on mac: brew install graphviz
    """)
    
if __name__ == '__main__':
    USAGE ="""
    1. execute a sql file, and draw its profile  
          python3 profile_viewer.py -f [path to sql file] -t [png title]
    2. draw a given profile
          python3 profile_viewer.py -qid [query_id] -t [png title]
    
    graphviz is required(https://graphviz.org/)
    on linux: apt install graphviz
    on mac: brew install graphviz
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="sql file", dest="sql_file", type=str, default ="")
    parser.add_argument("-qid", "--query_id", help="query id", dest="query_id", type=str, default="")
    parser.add_argument("-t", "--title", help="query title", dest="title", type=str, default="")

    args = parser.parse_args()
    if args.sql_file == ""  and args.query_id == "":
        print(USAGE)
        exit(0)
    title = args.title
    if args.query_id == "":
        query_id = exec_sql_and_draw(args.sql_file)
        draw_proile(query_id, title)
    else:
        draw_proile(args.query_id, title)

