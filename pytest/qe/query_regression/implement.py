#!/bin/env python
# -*- coding: utf-8 -*-
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

"""
for palo query test
Date: 2018-07-24 16:16:33
"""
import sys
import time
import optparse
import subprocess as commands
import json
import functools
sys.path.append("../..")

from lib import palo_client
from lib import palo_config
from lib import palo_logger


LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage
config = palo_config.config


def run_check(test_file, result_file):
    """
    base test
    """
    print("\t---------------------------------------------------------\t")
    print("input file:%s, result file:%s" % (test_file, result_file))
    LOG.info(L('input file: %s, result file: %s' % (test_file, result_file)))
    actual = _execute(test_file)
    if not actual:
        return False
    f_result = open(result_file, "r")
    lines = f_result.readlines()
    k = 0
    j = 0
    ret = True
    total_line = len(lines)
    for res in lines:
        res = res.strip()
        j += 1
        if actual[k].upper().endswith("#IGNORE CHECK"):
            if k >= total_line - 1:
                break
            k += 1
            continue
        if res.upper().endswith("#IGNORE CHECK") or res.startswith("--"):
            continue
        if actual[k] != res:
            if actual[k].startswith("False") and res.startswith("False"):
                print('-----------------diff info--------------------------------')
                print("different false info in result %d line" % j)
                print('expected: %s\nactual: %s' % (res, actual[k]))
                LOG.warning(L("different false info in result %d line" % j))
                LOG.warning(L('expected: %s\nactual: %s' % (res, actual[k])))
            else:
                ret = False
                print('****************diff result*******************************')
                print("different in result %d line" % j)
                LOG.error(L("different in result %d line" % j))
                if k > 0:
                    print(actual[k - 1])
                    LOG.error(L('this is wrong sql', sql=actual[k - 1]))
                print("expected: %s\nactual: %s" % (res, actual[k]))
                LOG.error(L("expected: %s but actual: %s" % (res, actual[k])))
               
        k += 1
    LOG.info(L('test %s result is %s' % (test_file, ret)))
    return ret


def gen_result(test_file, result_file):
    """
    gen result
    """
    result = _execute(test_file)
    f_result = open(result_file, "w")
    for r in result:
        f_result.write(r + "\n")

    f_result.close()
    return True


def compare(a, b):
    """compare data to None"""
    assert isinstance(a, (tuple, list))
    assert isinstance(b, (tuple, list))
    for i in range(0, len(a)):
        if a[i] is not None and b[i] is not None:
            if a[i] > b[i]:
                return 1
            elif a[i] < b[i]:
                return -1
            else:
                continue
        elif a[i] is None and b[i] is None:
            continue
        elif a[i] is None:
            return -1
        else:
            return 1
    return 0


def _execute(test_file):
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
            user=config.fe_user,
            password=config.fe_password)
    f_sql = open(test_file, "r")
    result = list()
    for sql in f_sql:
        IGNORE_CHECK = False
        sql = sql.strip()
        # 注释,忽略
        if not sql or sql.startswith("--"):
            continue

        # ignore this sql's result
        # select * from a#IGNORE CHECK ==> select * from a
        # ()                           ==> ()#IGNORE CHECK
        if sql.upper().endswith("#IGNORE CHECK"):
            sql = sql[:len(sql) - 13].strip()
            IGNORE_CHECK = True
        print(sql)
        LOG.info(L('execute', sql=sql))
        result.append(sql)

        # 对于use和导入语句,只记录SQL,不记录结果
        if sql.upper().startswith("USE"):
            selected_db = sql[4:].strip()
            client.use(selected_db)
            continue
        # 异步导入
        if sql.upper().startswith("LOAD") or sql.upper().startswith("INSERT"):
            try:
                client.execute(sql)
                client.wait_table_load_job()
            except Exception as e:
                print(str(e))
            continue
        # curl multi load & stream load
        if sql.upper().startswith("CURL"):
            try:
                sql = sql.format(FE_HOST=config.fe_host, HTTP_PORT=config.fe_http_port, FE_PASSWORD=config.fe_password)
                print(sql)
                ret = commands.getoutput(sql)
                print(ret)
                client.wait_table_load_job()
            except Exception as e:
                print(str(e))
                break
            continue

        # 不支持异常操作，execute的SQL应该是支持的
        r = client.execute(sql)
        # 排序
        if r:
            r = list(r)
            try:
                r.sort()
            except Exception as e:
                if isinstance(e, TypeError):
                    r.sort(key=functools.cmp_to_key(compare))
        # 字符串格式化
        r = str_format(r)
        actual = ("%s" % r).strip()

        if IGNORE_CHECK:
            actual += "#IGNORE CHECK"
        print(actual)
        result.append(actual)
    f_sql.close()
    return result


def str_format(result):
    """fromat result and return str"""
    ret = list()
    for line in result:
        record = list()
        for col in line:
            record.append(str(col))
        ret.append(record)
    return str(tuple(ret))
     

def check_cluster():
    """check be if enough"""
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
            user=config.fe_user,
            password=config.fe_password)
    alive_be = client.get_alive_backend_list()
    if len(alive_be) >= 3:
        return True
    else:
        return False
       

def main():
    """
    main
    """
    usage = "Usage: python implement.py test_file result_file"
    parser = optparse.OptionParser(usage)
    parser.add_option("-g", "--gen", dest="gen", default=False, action='store_true',
            help="generate result and write into file")
    options, args = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        return
    #s = run_check("atomic.sql", "atomic.result")
    if options.gen:
        s = gen_result(args[0], args[1])
        return
    s = run_check(args[0], args[1])
    print("--------------------------------------------------------------------------------------")
    print("Check Result: %s" % s)


if __name__ == "__main__":
    main()

