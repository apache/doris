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
import itertools
from typing import List, Dict

unary_op = ['~', '!', '~']
binary_op = ['+', '-', '*', '/', 'DIV', '%', '&', '|', '^']
bit_op = ['BITAND', 'BITOR', 'BITXOR']

type_to_column_map = {
    'TinyInt': ['ktint'],
    'SmallInt': ['ksint'],
    'Integer': ['kint'],
    'BigInt': ['kbint'],
    'LargeInt': ['klint'],
    'Float': ['kfloat'],
    'Double': ['kdbl'],
    'DecimalV2': ['kdcml'],
    'Char': ['kchr'],
    'Varchar': ['kvchr'],
    'String': ['kstr'],
    'Date': ['kdt'],
    'DateTime': ['kdtm'],
    'DateV2': ['kdtv2'],
    'DateTimeV2': ['kdtmv2'],
    'Boolean': ['kbool'],
}

type_set: Dict[str, List[str]] = {
    'int': ['Boolean', 'TinyInt', 'SmallInt', 'Integer', 'BigInt', 'LargeInt'],
    'float': ['Float', 'Double'],
    'string': ['Char', 'Varchar', 'String'],
    'date': ['Date', 'DateTime', 'DateV2', 'DateTimeV2']
}


def genBinaryExpr() -> List[str]:
    tables = ['expr_test', 'expr_test_not_nullable']
    count = 0
    SQLs = []
    for i in itertools.product(list(itertools.chain(*tuple(type_set.values()))), repeat=2):
        i = tuple(type_to_column_map[k][0] for k in i)
        cols = ', '.join([f'{i[0]} {t} {i[1]}' for t in binary_op])
        count += 1
        for tbl in tables:
            sql = f'select id, {cols} from {tbl} order by id'
            tag = f'qt_sql_test_{count}'
            if tbl != 'expr_test':
                tag += '_notn'
            SQLs.append(f'\t{tag} """\n\t\t{sql}"""\n')
    return SQLs


header = '''// Licensed to the Apache Software Foundation (ASF) under one
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

suite('nereids_arith_p0') {
    sql 'use regression_test_nereids_arith_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
'''

sqls = genBinaryExpr()
f = open('binary.groovy', 'w')
f.write(header)
f.writelines(sqls)
f.write('}')
f.close()
