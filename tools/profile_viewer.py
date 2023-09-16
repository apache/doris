import subprocess
import json
import requests
import os
import re
import argparse

FE_HOST = "http://127.0.0.1:8030"
MYSQL = "mysql -h:: -P9030 -u root "
DB = "tpch_sf1"
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
        
        m=re.search(" ([\w|\d]{16}-[\w|\d]{16}) ", self.merged.name)
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
        for child in self.children:
            if child.id != -1:
                content += "{}->{}[label={}]\n".format(child.caption, self.caption, child.rows)
        for child in self.children:
            if child.id != -1:
                content += child.to_dot()
        return content
            
    
def to_dot_file(root, dot_file):
    header = """
            digraph BPMN {
                rankdir=BT;
                node[style="rounded,filled" color="lightgoldenrodyellow" ]
            """
    tail = "}"
    with open(dot_file, 'w') as f:
        f.write(header)
        f.write(root.to_dot())
        f.write(tail)

def draw_proile(query_id):
    print("query_id: " + query_id)
    prof = get_profile(query_id)
    with open(WORK_DIR + query_id, "w") as f:
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
    dot_file = query_id + ".dot"
    png_file = query_id + ".png"

    to_dot_file(root_fr.merged.root, dot_file)
    cmd = "dot {} -T png -o {}".format(dot_file, png_file)
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
          python3 profile_viewer.py -sql [path to sql file]
    2. draw a given profile
          python3 profile_viewer.py -query_id [query_id]
    
    graphviz is required(https://graphviz.org/)
    on linux: apt install graphviz
    on mac: brew install graphviz
    """)
    
if __name__ == '__main__':
    USAGE ="""
    1. execute a sql file, and draw its profile  
          python3 profile_viewer.py -sql [path to sql file]
    2. draw a given profile
          python3 profile_viewer.py -query_id [query_id]
    
    graphviz is required(https://graphviz.org/)
    on linux: apt install graphviz
    on mac: brew install graphviz
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="sql file", dest="sql_file", type=str, default ="")
    parser.add_argument("-qid", "--query_id", help="query_id", dest="query_id", type=str, default="")
    args = parser.parse_args()
    print("sql_", args.sql_file)
    print("qid", args.query_id)
    if args.sql_file == ""  and args.query_id == "":
        print(USAGE)
        exit(0)
    if args.query_id == "":
        query_id = exec_sql_and_draw(args.sql_file)
        draw_proile(query_id)
    else:
        draw_proile(args.query_id)

