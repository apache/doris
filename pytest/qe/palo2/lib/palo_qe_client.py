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
query client
"""

import os
import sys
import json
import pymysql
import time
import pytest
import query_util as util

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage


class PaloQE(object):
    """
    function for query
    """
    def __init__(self, qe_host, qe_port=9030, qe_user="root@default_cluster",
            qe_passwd="", db_name="test_query_qa", mysql_charset="utf8"):
        self.qe_host=qe_host
        self.qe_port=qe_port
        self.qe_user=qe_user
        self.qe_passwd=qe_passwd
        self.db_name=db_name
        self.mysql_charset=mysql_charset

    def do_sql(self, sql):
        """
        run sql
        """
        connect = self.connect()
        try:
            connect.select_db(self.db_name)
        except Exception as e:
            print('use db failed ', str(e))
            LOG.error(L('use db failed', error=str(e)))
        try:
            cursor = connect.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            connect.close()
            print("palo execute succ")
            LOG.info('palo execute succ')
            return result
        except Exception as e:
            # import traceback
            # traceback.print_exc()
            # print(traceback.format_exc())
            print(str(e))
            LOG.error(L('Palo meet a error:', host=self.qe_host, port=self.qe_port, error=str(e)))
            raise PaloException("Palo meet a error", host=self.qe_host, port=self.qe_port, err=str(e))
  
    def do_set_properties_sql(self, sql, properties_to_set):
        """
        properties_to_set is a list
        set property and execute query
        """
        connect = self.connect()
        connect.select_db(self.db_name)
        cursor = connect.cursor()
        for property in properties_to_set:
            cursor.execute(property)
            LOG.info(L(property))
        LOG.info(L('query sql', sql=sql))
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        connect.close()
        return result
 
    def do_sql_with_mt_dop(self, sql, mt_dop=16):
        """
        set mt_dop and execute query
        """
        properties_list = list()
        properties_list.append("set mt_dop = %s" % mt_dop)
        LOG.info(L('sql:,properties_list', sql=sql, properties_list=properties_list))
        result = self.do_set_properties_sql(sql, properties_list)
        return result

    def do_set_sql(self, sql, mem_limit=2147483648, time_out=300, is_report=0, enable_spilling=0):
        """
        run sql
        """
        properties_list = list()
        properties_list.append("set exec_mem_limit = %d" % mem_limit)
        properties_list.append("set query_timeout = %d" % time_out)
        properties_list.append("set is_report_success = %d" % is_report)
        properties_list.append("set enable_spilling = %d" % enable_spilling)
        print("set mem %d time %d report %d spilling %d" % (mem_limit, time_out,
                                                            is_report, enable_spilling))
        LOG.info(L('sql, properties_list', sql=sql, properties_list=properties_list))
        result = self.do_set_properties_sql(sql, properties_list)
        return result

    def connect(self):
        """
        connect sql
        """
        if self.db_name is not None:
            curconnect = pymysql.connect(
                host=self.qe_host,
                port=self.qe_port,
                user=self.qe_user,
                passwd=self.qe_passwd,
                db=self.db_name,
                charset=self.mysql_charset)
        else:
            curconnect = pymysql.connect(
                host=self.qe_host,
                port=self.qe_port,
                user=self.qe_user,
                passwd=self.qe_passwd,
                charset=self.mysql_charset)
        return curconnect

    def do_sql_without_con(self, sql, con):
        """
        run sql
        """
        print(con.character_set_name())
        cursor = con.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result

    def get_query_plan(self, sql):
        """获取sql的查询计划"""
        if not sql.startswith("explain"):
            sql = "explain %s" % sql
        LOG.info(L("palo_sql", palo_sql = sql))
        res = self.do_sql(sql)
        return res


class PaloException(Exception):
    """
    Palo Exception
    """
    def __init__(self, message="", **kwargs):
        self.message = message
        self.kwargs = kwargs

    def __repr__(self):
        return "%s(message=%r, **%r)" % (self.__class__.__name__, self.message, self.kwargs)

    def __str__(self):
        return "%s %s" % (self.message, self.kwargs)


class QueryBase(object):
    def __init__(self):
        self.query_host = ''
        self.query_port = 9030
        self.query_db = "test_query_qa"
        self.query_user = "root"
        self.query_passwd = ""
        self.mysql_host = ''
        self.mysql_port = 3306
        self.mysql_db = "test_query_qa"
        self.mysql_user = "root"
        self.mysql_passwd = ""
        self.get_clients()

    def get_clients(self):
        """get palo query client and mysql connection"""
        if 'FE_HOST' in os.environ.keys():
            self.query_host = os.environ["FE_HOST"]
        if 'FE_QUERY_PORT' in os.environ.keys():
            self.query_port = int(os.environ["FE_QUERY_PORT"])
        if 'FE_USER' in os.environ.keys():
            self.query_user = os.environ["FE_USER"]
        if 'FE_PASSWORD' in os.environ.keys():
            self.query_passwd = os.environ["FE_PASSWORD"]
        if 'FE_DB' in os.environ.keys():
            self.query_db = os.environ["FE_DB"]
        print("--------------------------")
        print(self.query_host, self.query_port, self.query_user, self.query_passwd, self.query_db)
        LOG.info(L('palo connect info', query_host=self.query_host, query_port=self.query_port,
                   query_user=self.query_user, query_passwd=self.query_passwd, query_db=self.query_db))
        print("--------------------------")

        if 'MYSQL_HOST' in os.environ.keys():
            self.mysql_host = os.environ["MYSQL_HOST"]
        if 'MYSQL_PORT' in os.environ.keys():
            self.mysql_port = int(os.environ["MYSQL_PORT"])
        if 'MYSQL_USER' in os.environ.keys():
            self.mysql_user = os.environ["MYSQL_USER"]
        if 'MYSQL_PASSWORD' in os.environ.keys():
            self.mysql_passwd = os.environ["MYSQL_PASSWORD"]
        if 'MYSQL_DB' in os.environ.keys():
            self.mysql_db = os.environ["MYSQL_DB"]
        print("--------------------------")
        print(self.mysql_host, self.mysql_port, self.mysql_user, self.mysql_passwd, self.mysql_db)
        LOG.info(L('mysql connect', mysql_host=self.mysql_host, mysql_port=self.mysql_port,
                   mysql_user=self.mysql_user, mysql_passwd=self.mysql_passwd, mysql_db=self.mysql_db))
        print("--------------------------")

        self.query_palo = PaloQE(self.query_host, self.query_port, self.query_user, 
                                 self.query_passwd, self.query_db)
        self.mysql_con = pymysql.connect(host=self.mysql_host, user=self.mysql_user,
                                         passwd=self.mysql_passwd,
                                         port=self.mysql_port, db=self.mysql_db)
        self.mysql_cursor = self.mysql_con.cursor()
        try:
            self.query_palo.do_sql('select count(*) from baseall')
            self.query_palo.do_sql('select count(*) from test')
        except PaloException as e:
            if 'have no queryable replicas' in str(e) or 'There is no scanNode Backend' in str(e):
                raise pytest.skip("some be maybe dead, skip test case")

    def check(self, sql, force_order=False):
        """check sql execute right"""
        print(sql)
        times = 0
        flag = 0
        while times <= 10 and flag == 0:
            try:
                LOG.info(L('palo sql', palo_sql=sql))
                palo_result = self.query_palo.do_sql(sql)
                LOG.info(L('mysql sql', mysql_sql=sql))
                self.mysql_cursor.execute(sql)
                mysql_result = self.mysql_cursor.fetchall()
                util.check_same(palo_result, mysql_result, force_order)
                flag = 1
            except PaloException as e:
                LOG.error(L('error', error=e))
                time.sleep(1)
                times += 1
                if times == 3:
                    LOG.error(L('error', error=e))
                    assert 0 == 1, 'check error'

    def check2(self, psql, msql, force_order=False):
        """check palo and mysql execute sql same"""
        print(psql)
        print(msql)
        times = 0
        flag = 0
        while times <= 10 and flag == 0:
            try:
                LOG.info(L('palo sql', palo_sql=psql))
                palo_result = self.query_palo.do_sql(psql)
                LOG.info(L('mysql sql', mysql_sql=msql))
                self.mysql_cursor.execute(msql)
                mysql_result = self.mysql_cursor.fetchall()
                util.check_same(palo_result, mysql_result, force_order)
                flag = 1
            except PaloException as e:
                LOG.error(L('error', error=e))
                time.sleep(1)
                times += 1
                if times == 3:
                    assert 0 == 1, 'check error'

    def check2_palo(self, sql_1, sql_2, force_order=False):
        """check 2个sql execute right"""
        times = 0
        flag = 0
        print(sql_1)
        print(sql_2)
        while times <= 10 and flag == 0:
            try:
                LOG.info(L('palo sql_1', palo_sql_1=sql_1))
                palo_result = self.query_palo.do_sql(sql_1)
                LOG.info(L('palo sql_2', palo_sql_2=sql_2))
                mysql_result = self.query_palo.do_sql(sql_2)
                util.check_same(palo_result, mysql_result, force_order)
                flag = 1
            except PaloException as e:
                time.sleep(1)
                times += 1
                if times == 3:
                    LOG.error(L('error', error=e))
                    assert 0 == 1, 'check error'

    def check2_diff(self, psql, msql, diff=0.1, accurate_column=[], force_order=False):
        """check palo and mysql execute sql same,allow diff percenty is 0.1
        accurate_column:精确check的列,1是精算，0是估算：[0,1,0,1]表示第一，三列估算
        """
        print(psql)
        print(msql)
        times = 0
        flag = 0
        while times <= 10 and flag == 0:
            try:
                LOG.info(L('palo sql', palo_sql=psql))
                palo_result = self.query_palo.do_sql(psql)
                LOG.info(L('mysql sql', mysql_sql=msql))
                self.mysql_cursor.execute(msql)
                mysql_result = self.mysql_cursor.fetchall()
                ###actucal vs estimate diff in 1%
                same, gap = util.check_percent_diff(palo_result, mysql_result, accurate_column, force_order, diff)
                if same == False:
                    assert 0 == 1, "actual max diff %s, excepted diff count %s"\
                        % (gap, diff)
                flag = 1
            except PaloException as e:
                print(str(e))
                print("hello")
                LOG.error(L('error', error=e))
                time.sleep(1)
                times += 1
                if times == 3:
                    assert 0 == 1, 'check error'

    def checkok(self, sql):
        """check sql execute ok"""
        print(sql)
        times = 0
        flag = 0
        while times <= 10 and flag == 0:
            try:
                LOG.info(L('palo sql', palo_sql=sql))
                result = self.query_palo.do_sql(sql)
                flag = 1
            except PaloException as e:
                print(str(e))
                LOG.error(L('error', error=e))
                time.sleep(1)
                times += 1
                if times == 3:
                    assert 0 == 1, 'check error, expect sql execute success'

    def init(self, sql, msql=None):
        """palo and mysql execute sql to init env"""
        LOG.info(L('palo sql', palo_sql=sql))
        result = self.query_palo.do_sql(sql)
        if msql is None:
            msql = sql
        else:
            print(msql)
        LOG.info(L('mysql sql', mysql_sql=msql))
        self.mysql_cursor.execute(msql)
        mysql_result = self.mysql_cursor.fetchall()
        self.mysql_con.commit()
        LOG.info(L('mysql execute succ'))

    def checkwrong(self, sql, msg=None):
        """check sql execute wrong"""
        print(sql)
        times = 0
        flag = 0
        try:
            LOG.info(L('palo sql', palo_sql=sql))
            palo_result = self.query_palo.do_sql(sql)
        except PaloException as e:
            print(str(e))
            LOG.info(L('error', error=e))
            if msg is not None:
                assert msg in str(e), 'expect error msg: %s ,\nactual msg: %s' % (msg, str(e))
                flag = 1
            else:
                # 如遇到以下错误，认为不符合预期
                if str(e).find('maybe this is a bug') == -1 and \
                   str(e).find('Lost connection') == -1 and \
                   str(e).find('MySQL server has gone away') == -1:
                    flag = 1
        assert flag == 1, 'check error, expect sql not support and error msg ok'

    def wait_end(self, job=None):
        """wait load/schema change job end"""
        timeout = 1200
        LOG.info(L('wait job end'))
        while timeout > 0:
            if job.upper() == 'LOAD':
                sql = 'show load'
                LOG.info(L('palo sql', palo_sql=sql))
                job_list = self.query_palo.do_sql(sql)
                state = job_list[-1][2]
            elif job.upper() == 'SCHEMA_CHANGE':
                sql = 'show alter table column'
                job_list = self.query_palo.do_sql(sql)
                state = job_list[-1][9]

            if state == "FINISHED" or state == "CANCELLED":
                # return state == "FINISHED"
                timeout = 0
            time.sleep(1)
            timeout -= 1
        LOG.info(L('assert state', state=state))
        assert state == 'FINISHED', "res %s, excepted 'FINISHED'" % state

    def __del__(self):
        self.mysql_cursor.close()

    def get_sql_result(self, sql, is_palo=True):
        """execute sql & get result"""
        if is_palo:
             LOG.info(L('palo sql', palo_sql=sql))
             ret = self.query_palo.do_sql(sql)
        else:
             LOG.info(L('mysql sql', mysql_sql=sql))
             self.mysql_cursor.execute(sql)
             ret = self.mysql_cursor.fetchall()
        return ret


if __name__ == "__main__":
    query_host = ''
    query_port = 9030
    query_db = "test_query_qa"
    query_user = "root"
    query_passwd = ""

    if 'FE_HOST' in os.environ.keys():
        query_host = os.environ["FE_HOST"]
    if 'FE_QUERY_PORT' in os.environ.keys():
        query_port = int(os.environ["FE_QUERY_PORT"])
    if 'FE_USER' in os.environ.keys():
        query_user = os.environ["FE_USER"]
    if 'FE_PASSWORD' in os.environ.keys():
        query_passwd = os.environ["FE_PASSWORD"]
    if 'FE_DB' in os.environ.keys():
        query_db = os.environ["FE_DB"]

    query_palo = PaloQE(query_host, query_port, query_user, query_passwd, query_db)
    sql = 'SHOW LOAD FROM %s' % query_db
    ret = query_palo.do_sql(sql)
    flag = True
    retry_times = 600
    while flag == True and retry_times > 0:
        flag = True
        for load_job in ret:
            if load_job[2] != 'FINISHED':
                flag = False
        retry_times -= 1
        time.sleep(2)
    if retry_times == 0:
        exit(-1)
