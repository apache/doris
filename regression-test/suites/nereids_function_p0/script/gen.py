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

# usage: run this script and paste the output to corresponding suite.

import os
import re
from typing import List
DORIS_HOME = "../../../"
mp = {}
mp1 = {'tinyint': ['ktint'],
       'smallint': ['ksint'],
       'integer': ['kint'],
       'bigint': ['kbint'],
       'largeint': ['klint'],
       'float': ['kfloat'],
       'double': ['kdbl'],
       'decimalv2': ['kdcmls1'],
       'char': ['kchrs1'],
       'varchar': ['kvchrs1'],
       'string': ['kstr'],
       'date': ['kdt'],
       'datetime': ['kdtm'],
       'datev2': ['kdtv2'],
       'datetimev2': ['kdtmv2s1'],
       'boolean': ['kbool'],
       'st_string': ['st_point_str'],
       'st_varchar': ['st_point_vc'],
       '': ['']}

def run(path):
    for f in os.listdir(path):
        fpath = os.path.join(path, f)
        if os.path.isdir(fpath):
            run(fpath)
        else:
            name = to_snake_case(f[:-5])
            text = open(fpath, "r").read().replace('\n', '').replace('\t', '').replace(' ', '').replace(',', ', ')
            text = text[text.find('.of('):]
            text = text[:text.find(');')]
            if len(text) == 0 or text.find('Array') != -1:
                continue
            lines = text.replace('Type', '').replace('.SYSTEM_DEFAULT', '').replace('.INSTANCE', '').split(
                'FunctionSignature')[1:]
            lines = [((s[s.find('rgs'):]).replace('rgs(', ', ')
                      .replace(').varArgs(', ', '))[:-1].replace(')),', ')') for s in lines]
            for i in range(0, len(lines)):
                if lines[i][-1] == ',':
                    lines[i] = lines[i][:-2]
            lines = [[to_snake_case(s1).replace('_', '') for s1 in s.split(', ')] for s in lines]
            mp[name] = lines

def generete_args(func_name: str, args_type: List[str]):
    need_change = lambda x : x == 'string' or x == 'varchar'
    if "st_" == func_name[:3]:
        need_change = lambda x : x == 'string' or x == 'varchar'
        args_type = ["st_"+t if need_change(t) else t for t in args_type]
    return [mp1[s][0] for s in args_type]

def to_snake_case(camel_case: str):
    snake_case = re.sub(r"(?P<key>[A-Z])", r"_\g<key>", camel_case)
    return snake_case.lower().strip('_')

run(f'{DORIS_HOME}/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/functions/scalar')
for k in sorted(mp):
    v = mp[k]
    args = ''
    for i in v:
        flag = 0
        for j in i[1:]:
            if mp1.get(j) == None:
                flag = 1
                break
        if flag == 1:
            print('// function ' + k + '(' + ", ".join(i[1:]) + ') is unsupported for the test suite.')
            continue
        args = ", ".join(generete_args(k, list(i[1:])))
        print('sql "select ' + k + '(' + args + ') from fn_test order by ' + args + '"')

