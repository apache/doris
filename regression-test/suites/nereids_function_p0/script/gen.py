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
from typing import List, Tuple, Dict, Callable
import define

DORIS_HOME = "../../../../"
type_to_column_map = {
    'TinyInt': ['ktint'],
    'SmallInt': ['ksint'],
    'Integer': ['kint'],
    'BigInt': ['kbint'],
    'LargeInt': ['klint'],
    'Float': ['kfloat'],
    'Double': ['kdbl'],
    'DecimalV2': ['kdcmls1'],
    'Char': ['kchrs1'],
    'Varchar': ['kvchrs1'],
    'String': ['kstr'],
    'Date': ['kdt'],
    'DateTime': ['kdtm'],
    'DateV2': ['kdtv2'],
    'DateTimeV2': ['kdtmv2s1'],
    'Boolean': ['kbool'],
    'st_string': ['st_point_str'],
    'st_varchar': ['st_point_vc'],
    'QuantileState': ['to_quantile_state(kvchrs1, 2048)', 'kvchsr1'],
    'Bitmap': ['to_bitmap(kbint)', 'kbint'],
    'AnyData': ['kint'],
    'Hll': ['hll_raw_agg(kint)', 'kint'],
    '': ['']
}


# def getFunctionSet() -> List[str]:
#     path = f'{DORIS_HOME}fe/fe-core/src/main/java/org/apache/doris/catalog/'
#     functions = []
#     dir_list = os.listdir(path)
#     for file_name in dir_list:
#         file_path_name = os.path.join(path, file_name)
#         if not os.path.isdir(file_path_name):
#             file_name = file_name[:-5]
#             if file_name.startswith('Builtin') and file_name.endswith('Functions'):
#                 lines = open(file_path_name).readlines()
#                 for line in lines:
#                     line = line[line.find('(') + 1: max(line.find('.class'), 0)]
#                     if line != '':
#                         functions.append(line)
#     return functions


def checkSupportedFunction(args: List[str]) -> bool:
    for arg in args:
        if arg.find('DecimalV3') != -1 or arg.find('Array') != -1 or arg.find('Json') != -1:
            return False
    return True


def extractNameForFunction(name_text: str) -> str:
    name_text: List[str] = re.findall('super\(\"([a-z_0-9]+)\",', name_text)
    if len(name_text) == 0:
        return None
    fn_name = name_text[0]
    if fn_name.find('json') != -1 or fn_name.find('array') != -1:
        return None
    if fn_name in define.denied_tag:
        return None
    return fn_name


def extractReturnTypeAndArgTypesForFunction(text: str) -> Tuple[str, List[List[str]]]:
    text = text.replace('\n', '').replace('\t', '').replace(' ', '').replace(',', ', ')
    text = text[text.find('.of('):]
    name_text = extractNameForFunction(text)
    if name_text is None:
        return None

    text = text[:text.find(');')]
    if len(text) == 0 or text.find('Array') != -1:
        return None

    lines = [s[s.find('(') + 1: s.find(')')] for s in re.findall('(?:args|varArgs)\([A-Za-z._0-9\ ,]*\)', text)]
    lines = [[s1[:s1.find('Type.')] for s1 in s.split(', ')] for s in lines]
    lines = [s for s in lines if checkSupportedFunction(s)]

    return name_text, lines


def searchFunctions(path: str, func: Callable[[str], bool]) -> Dict[str, List[List[str]]]:
    functions = {}
    for file_name in os.listdir(path):
        file_path_name = os.path.join(path, file_name)
        if os.path.isdir(file_path_name):
            functions.update(searchFunctions(path + file_name, func))
            continue
        if not func(file_name):
            continue
        function_tag = extractReturnTypeAndArgTypesForFunction(open(file_path_name).read())
        if function_tag is None:
            continue
        fn_name, args = function_tag
        functions[fn_name] = args
    return functions


def getSQLMeta(fn_name: str, args: List[str]) -> Tuple[str, str, str, str]:
    fn_title = f'{fn_name}{"_" + "_".join(args) if args[0] != "" else ""}'
    trans_args = [type_to_column_map[s][0] for s in args]
    column_args = [type_to_column_map[s][0 if len(type_to_column_map[s]) == 1 else 1] for s in args]

    args = ', '.join(trans_args)
    run_tag = f'{"sql" if fn_title in define.not_check_result else f"qt_sql_{fn_title}"}'

    order_by_args = ', '.join(column_args)
    order_by = f' order by {order_by_args}' if trans_args[0] != "" else ""
    return fn_title, args, run_tag, order_by


def generateScalarFnSQL(function_meta: Dict[str, List[List[str]]]) -> List[str]:
    tables = ['fn_test', 'fn_test_not_nullable']
    SQLs = []
    for fn_name in sorted(function_meta):
        for args in function_meta[fn_name]:
            fn_title, args, run_tag, order_by = getSQLMeta(fn_name, args)

            for t in tables:
                if t != 'fn_test' and run_tag != 'sql':
                    run_tag += '_notnull'
                if fn_title in define.const_sql:
                    sql = define.const_sql[fn_title]
                    sql = sql.replace('${t}', t)
                else:
                    sql = f'select {fn_name}({args}) from {t}{order_by}'

                SQLs.append(f'\t{run_tag} "{sql}"\n')
            SQLs.append('\n')
    return SQLs


def generateAggFnSQL(function_meta: Dict[str, List[List[str]]]) -> List[str]:
    tables = ['fn_test', 'fn_test_not_nullable']
    SQLs = []
    for fn_name in sorted(function_meta):
        for args in function_meta[fn_name]:
            fn_title, args, run_tag, _ = getSQLMeta(fn_name, args)
            group_by = ' group by kbool'
            order_by = ' order by kbool'

            for t in tables:
                for i in range(0, 2):
                    tag_app = ""
                    if t != 'fn_test' and run_tag != 'sql':
                        tag_app += '_notnull'
                    if i == 0:
                        tag_app += '_gb'
                    if fn_title in define.const_sql:
                        sql = define.const_sql[fn_title]
                        sql = sql.replace('${t}', t)
                    else:
                        sql = f'select {fn_name}({args}) from {t}'
                    sql += f'{group_by + order_by if i == 0 else ""}'

                    SQLs.append(f'\t{run_tag + tag_app} "{sql}"\n')
            SQLs.append('\n')
    return SQLs


def generateGenFnSQL(function_meta: Dict[str, List[List[str]]]) -> List[str]:
    tables = ['fn_test', 'fn_test_not_nullable']
    SQLs = []
    for fn_name in sorted(function_meta):
        for args in function_meta[fn_name]:
            fn_title, args, run_tag, order_by = getSQLMeta(fn_name, args)

            for t in tables:
                if t != 'fn_test' and run_tag != 'sql':
                    run_tag += '_notnull'
                if fn_title in define.const_sql:
                    sql = define.const_sql[fn_title]
                    sql = sql.replace('${t}', t)
                else:
                    sql = f'select id, e from {t} lateral view {fn_name}({args}) lv as e'

                SQLs.append(f'\t{run_tag} "{sql}"\n')
            SQLs.append('\n')
    return SQLs


def genHeaderAndFooter(tag: str,
                       input_dir: str, output_file: str, title: str, func: Callable[[str], bool]) -> bool:
    open_nereids = True
    sqls = fn_tag[tag](searchFunctions(input_dir, func))
    if len(sqls) == 0:
        return True
    f = open(output_file, 'w')
    f.write(define.header)
    f.write(f'suite("{title}") ''{\n'
            '\tsql \'use regression_test_nereids_function_p0\'\n'
            f'\tsql \'set enable_nereids_planner={"true" if open_nereids else "false"}\'\n'
            '\tsql \'set enable_fallback_to_original_planner=false\'\n')
    f.writelines(sqls)
    f.write('}')
    f.close()
    return True


fn_tag = {
    'agg': generateAggFnSQL,
    'scalar': generateScalarFnSQL,
    'gen': generateGenFnSQL,
}

getChar: Callable[[int], Callable[[str], bool]] = lambda c: \
    lambda s: s[s.rfind('/') + 1: s.rfind('.')][0] == c

getCharRange: Callable[[int, int], Callable[[str], bool]] = lambda c1, c2: \
    lambda s: c1 <= s[s.rfind('/') + 1: s.rfind('.')][0] <= c2

FUNCTION_DIR = '../../../../fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/functions/'

genHeaderAndFooter('gen',
                   f'{FUNCTION_DIR}generator',
                   f'../gen_function/gen.groovy',
                   f'nereids_gen_fn',
                   lambda c: True)
