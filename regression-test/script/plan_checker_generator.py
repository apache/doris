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

from curses.ascii import isalpha
import sys
import re
import os

# line format is "sql_name, groovy_name"

suite = """// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_explain_tpch_sf_1_q{}", "tpch_sf1") {{
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${{realDb}}"
"""

params = sys.argv

sql_dir = params[1]

output_dir = params[2]

if not os.path.exists(output_dir):
    os.mkdir(output_dir)

sql_files = [f for f in os.listdir(sql_dir) if os.path.isfile(os.path.join(sql_dir, f))]

sql_numbers = [re.findall(r'\d+', f)[0] for f in sql_files]

output_files = [os.path.join(output_dir, 'test_'+x[1] + '.groovy') for x in zip(sql_files, sql_numbers)]

sql_files_path = [os.path.join(sql_dir, p) for p in sql_files]

tasks = zip(sql_files_path, output_files, sql_numbers)

patterns = [
    'order by.*$',
    'runtime filters.*$',
    'VTOP-N',
    'join op.*$',
    'equal join.*$',
    'TABLE.*$',
    'PREDICATES.*$',
    'output.*$',
    'other join predicates.*$',
    'other predicates.*$',
    'group by.*$',
    'VAGGREGATE.*$',
    'STREAMING',
    'cross join.*$',
    'predicates.*$',
]

for task in tasks:

    print(task)
    f1, f2, num = task

    f = open(f1, 'r')
    oldsql = f.read()
    f.close()

    f = open(f1, 'w')
    f.write('explain\n' + oldsql + ';')
    f.close()

    conn = 'mysql -h 127.0.0.1 -P 9030 -uroot regression_test_tpch_sf1 <'

    print(conn + f1)

    lines = os.popen(conn + f1).readlines()

    f = open(f1, 'w')
    f.write(oldsql)
    f.close()

    # print(oldsql)

    sqls = open(f1).readlines()

    print(len(lines))

    res_pattern = []

    res_g = []

    for id, line in enumerate(lines):
        lines[id] = line.rstrip('\n')

    res = []

    cur = []
    for id, line in enumerate(lines):
        # print(g)
        line = line.replace('$','\$')
        for p in patterns:
            if len(re.findall(p, line)) > 0: #and line.find('null') == -1:
                cur.append((line, id))

    # print(cur)    
    res_g = [[cur[0][0]]]
    for i in range(1, len(cur)):
        if cur[i][1] == cur[i - 1][1] + 1:
            res_g[-1].append(cur[i][0])
        else:
            res_g.append([cur[i][0]])

    for g in res_g:
        s = g[0]
        while not isalpha(s[0]):
            s = s[1:]
        g[0] = s

    explain_pattern = '''
        explainStr -> 
            explainStr.contains('''

    explain_pattern1 = ''') &&
            explainStr.contains('''

    chkstr = ''
    for g in res_g:
        chkstr = chkstr + '\t\t' + 'explainStr.contains("' + g[0]
        if len(g) == 1:
            chkstr = chkstr + '") && \n'
            continue
        else:
            chkstr = chkstr + '\\n" + \n'
        for s in g[1:-1]:
            chkstr = chkstr + '\t\t\t\t"' + s + '\\n" + \n'
        chkstr = chkstr + '\t\t\t\t"' + g[-1] + '") && \n'

    chkstr = chkstr[:-4]
    pattern = '''
    explain {
            sql """
'''
    pattern1 = '''
            """
        check {
            explainStr ->
'''

    pattern2 = '''
            
        }
    }
}'''

    sql = ''
    for line in sqls[1:]:
        sql = sql + '\t\t' + line

    # print(suite.format(num) + pattern + sql + pattern1 + chkstr + pattern2)
    open(f2, 'w').write(suite.format(num) + pattern + sql + pattern1 + chkstr + pattern2)