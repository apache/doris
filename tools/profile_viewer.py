
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

import re
import os
import sys
#dot dig -T png -o dig.png

exchanges = {}

class Node:
    TERMINAL_NODE_TYPES={'VEXCHANGE_NODE', 'VNewOlapScanNode'}
    def __init__(self, id, type, frid):
        self.id = id
        self.frid = frid
        self.type = type
        self.parent = None
        self.children = []
        self.rows = 0
        m = re.search("VNewOlapScanNode\((.*)\)", type)
        if m != None:
            self.table = m.group(1)
            self.type = 'VNewOlapScanNode'
        else:
            self.table = ""

    def set_data(self, data):
        self.data = data

    def parse(self):
        for line in self.data:
            m = re.search(".*RowsReturned:.+\(([\d]+)\)", line)
            if m!= None:
                self.rows = int(m.group(1))
                break
            else:
                m = re.search(".*RowsReturned:.+([\d]+)", line)
                if m != None:
                    self.rows = int(m.group(1))
                    break

    def build_tree(self, nodes):
        if self.type in Node.TERMINAL_NODE_TYPES:
            return 0
        self.children.append(nodes[0])
        consume = 1 + nodes[0].build_tree(nodes[1:])
        if self.type.find("JOIN") > 0:
            self.children.append(nodes[consume])
            consume = consume + 1 + nodes[consume].build_tree(nodes[consume+1:])
        return consume

    def __repr__(self):
        if self.type == 'VNewOlapScanNode':
            return "{}_{}".format(self.table, self.id)
        return "{}_{}".format(self.type, self.id)

    def to_label(self):
        name = self.type
        if self.type == 'VNewOlapScanNode':
            name = self.table
        return '"{}\\n#{}FR{}"'.format(name, self.id, self.frid)

    def show(self, indent):
        print("{}{}".format(indent, self))
        for ch in self.children:
            ch.show("  " + indent)

    def to_dot(self, dot):
        dot = dot + "\n{}[label={}]".format(self, self.to_label())
        for ch in reversed(self.children):
            dot = dot + "\n{}->{}[label={}]".format(ch, self, ch.rows)
        for ch in self.children:
            dot = ch.to_dot(dot)
        return dot
class Instance:
    def __init__(self, id, frid):
        self.id = id
        self.frid = frid
        self.target = ""
        self.srcs = []
        self.nodes = []
        self.has_build_tree = False

    def set_data(self, data):
        self.data = data

    def build_tree(self):
        if self.has_build_tree:
            return
        self.has_build_tree = True
        self.root = self.nodes[0]
        self.root.build_tree(self.nodes[1:])
        
    def parse(self):
        pos = 0
        for line in self.data:
            pos = pos + 1
            m = re.search("\s+VDataStreamSender  \(dst_id=(\d+),.*", line)
            if m != None:
                self.target = m.group(1)
                break

        for line in self.data:
            m = re.search("\s+VEXCHANGE_NODE  \(id=(\d+)\):.*", line)
            if m != None:
                self.srcs.append(m.group(1))
        
        prev = 0
        pos = 0
        for line in self.data:
            m = re.search("\s+([^\s]+N[o|O][d|D][e|E][^\s]*)  \(id=(\d+)\.*", line)
            if m != None:
                type = m.group(1)
                id = m.group(2)
                node = Node(id, type, self.frid)
                if node.type == 'VEXCHANGE_NODE':
                    exchanges[id] = node
                if prev != 0:
                    #self.nodes
                    self.nodes[-1].set_data(self.data[prev : pos])
                self.nodes.append(node)
                prev = pos
            pos = pos + 1
        self.nodes[-1].set_data(self.data[prev : pos])

        for node in self.nodes:
            node.parse()
        self.build_tree()


    def show(self):
        self.root.show(" ")

class Fragment:
    def __init__(self, id):
        self.id = id
        self.target = ""
        self.srcs = []
        self.instances = []
        self.children = []
        self.parent = None

    def set_data(self, data):
        self.data = data
    
    def get_instance_id(self, line):
        m = re.search("\s+Instance  (\w+-\w+)", line)
        is_inst = m != None
        return (is_inst, m.group(1) if is_inst else "")

    def parse(self):
        pos = 1
        (is_inst, inst_id) = self.get_instance_id(self.data[pos])
        inst = Instance(inst_id, self.id)
        self.instances.append(inst)
        size = len(self.data)
        prev = pos
        while pos < size - 1:
            pos = pos + 1
            (is_inst, inst_id) = self.get_instance_id(self.data[pos])
            if is_inst:
                inst.set_data(self.data[prev : pos])
                inst = Instance(inst_id, self.id)
                self.instances.append(inst)
                prev = pos
            
        inst.set_data(self.data[prev : pos])    
        
        for inst in self.instances:
            inst.parse()
        self.srcs = self.instances[0].srcs
        self.target = self.instances[0].target


    def show(self, indent):
        print("{}{}".format(indent, self.id))
        for fr in self.children:
            fr.show(indent+"  ")

    def sum_nodes(self, nodes):
        sum = 0
        for node in nodes:
            sum = sum + node.rows
        nodes[0].rows = sum
        for (i, ch) in enumerate(nodes[0].children):
            chs = [node.children[i] for node in nodes]
            self.sum_nodes(chs)

    def zip_instances(self, instances):
        '''sum rows of same nodes in different instance'''
        roots = [inst.root for inst in instances]
        self.sum_nodes(roots)

    def to_dot(self):
        inst = self.instances[0]
        dot = ""
        dot = inst.root.to_dot(dot)
        return dot

class Tree:
    def __init__(self, fragments):
        self.fragments = fragments

    def build(self):
        src_fr_map = {}
        for fr in self.fragments:
            if fr.target == "":
                self.root = fr
            for src in fr.srcs:
                src_fr_map[src] = fr
        for fr in self.fragments:
            if fr.target != "":
                parent = src_fr_map.get(fr.target)
                
                parent.children.append(fr)
                fr.parent = parent

    def show(self):
        self.root.show("  ")    

def load_profile(profile):
    with open(profile, 'r') as f:
        l = f.readlines()
        return l

def get_fragment_id(line):
    #line in format : Fragment /d+:
    #len("Fragment  "):10
    line = line.strip()
    return line[10:-1]

def is_fragment_begin(line):
    line = line.strip()
    return line.startswith("Fragment")

def cut_fragment(data):
    fragments = []
    prev = 0
    pos = 0
    total_len = len(data)
    #skip some lines not in fragment
    for line in data:
        if is_fragment_begin(line):
            break
        prev = prev + 1
    pos = prev
    fr_id = get_fragment_id(data[pos])
    pos = pos + 1
    current_fr = Fragment(fr_id)
    fragments = []
    fragments.append(current_fr)
    while pos < total_len:
        line = data[pos]
        if is_fragment_begin(line):
            current_fr.set_data(data[prev : pos])
            fr_id = get_fragment_id(line)
            current_fr = Fragment(fr_id)
            fragments.append(current_fr) 
            prev = pos
            
        pos = pos + 1

    current_fr.set_data(data[prev: pos - 1])
    for fr in fragments:
        fr.parse()
    return fragments

def print_usage():
    print("""
    USAGE:
        python profile_viewer.py [PATH_TO_PROFILE]
    
    graphviz is required(https://graphviz.org/)
    on linux: apt install graphvize
    on mac: brew install graphvize
    """)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print_usage()
        exit(0)
    prf_file = sys.argv[1]
    if not os.path.exists(prf_file):
        print("{} is not a file".format(prf_file))
        exit(0)

    data = load_profile(prf_file)
    fragements = []
    fragments = cut_fragment(data)
    dot_header = """digraph BPMN {
    rankdir=BT;
    node[shape=rectanble style="rounded,filled" color="lightgoldenrodyellow" ]
    """
    dot_tail = "}"
    with open(prf_file+".dot", "w") as f:
        f.write(dot_header)
        for fr in fragments:
            # print("fr{}:{} <-{}".format(fr.id, fr.target, fr.srcs))
            fr.zip_instances(fr.instances)
            f.write(fr.to_dot())
            if fr.target != "":
                f.write("\n{}->{}[label={}]".format(fr.instances[0].root, exchanges[fr.target], fr.instances[0].root.rows))
        f.write(dot_tail)
    cmd = "dot {} -T png -o {}".format(prf_file+".dot", prf_file+".png")
    status = os.system(cmd)
    if status != 0:
        print("convert failed")
    else:
        print("generate " + prf_file + ".png")
        



