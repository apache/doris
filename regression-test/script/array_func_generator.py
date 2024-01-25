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

def generate_test_queries(func_name, lambda_name):
    data_types = {
        'Double': 'kadbl',
        'Float': 'kafloat',
        'LargeInt': 'kalint',
        'BigInt': 'kabint',
        'SmallInt': 'kasint',
        'Integer': 'kaint',
        'TinyInt': 'katint',
        'DecimalV3': 'kadcml'
    }

    table_names = {
        'Double': 'fn_test',
        'Float': 'fn_test',
        'LargeInt': 'fn_test',
        'BigInt': 'fn_test',
        'SmallInt': 'fn_test',
        'Integer': 'fn_test',
        'TinyInt': 'fn_test',
        'DecimalV3': 'fn_test'
    }

    queries = []
    for data_type, column_name in data_types.items():
        query = f'order_qt_sql_{func_name}_{data_type} "select {func_name}({lambda_name}, {column_name}) from {table_names[data_type]}"'
        query_not_null = f'order_qt_sql_{func_name}_{data_type}_notnull "select {func_name}({lambda_name}, {column_name}) from {table_names[data_type]}_not_nullable"'
        queries.append(query)
        queries.append(query_not_null)

    return queries

# 调用函数生成测试查询
func_name_set = {"array_map", "array_filter", "array_exists", "array_count", "array_last_index", "array_first_index", "array_sortby"}
queries = []
for func_name in func_name_set:
    queries.append(f"// test {func_name}")
    queries.extend(generate_test_queries(func_name, "x -> x > 1"))

for query in queries:
    print(query)