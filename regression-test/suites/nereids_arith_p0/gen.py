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
    'Char': ['kchrs1'],
    'Varchar': ['kvchrs1'],
    'String': ['kstr'],
    'Date': ['kdt'],
    'DateTime': ['kdtm'],
    'DateV2': ['kdtv2'],
    'DateTimeV2': ['kdtmv2'],
    'Boolean': ['kbool'],
}

type_set = {
    'int': ['Boolean', 'TinyInt', 'SmallInt', 'Integer', 'BigInt', 'LargeInt'],
    'float': ['Float', 'Double'],
    'string': ['Char', 'Varchar', 'String'],
    'date': ['Date', 'DateTime', 'DateV2', 'DateTimeV2']
}

special_ret_map = {

}
