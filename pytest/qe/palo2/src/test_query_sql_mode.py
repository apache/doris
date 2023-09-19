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
this file is to test the function about sql mode
"""
import pymysql
import sys
import time
sys.path.append("../lib/")
from palo_qe_client import QueryBase
import query_util as util

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage


table_name = "test"
join_name = "baseall"

def setup_module():
    """
    init config
    """
    global runner
    runner = QueryExecuter()


class QueryExecuter(QueryBase):
    def __init__(self):
        self.get_clients()

    def do_sql_mysql(self, sql, properties_to_set):
        """ 
        properties_to_set is a list
        set property and execute query
        """
        self.connect = pymysql.connect(
            host=self.mysql_host,
            port=self.mysql_port,
            user=self.mysql_user,
            passwd=self.mysql_passwd,
            connect_timeout=1000)
        self.connect.select_db(self.mysql_db)
        cursor = self.connect.cursor()
        for property in properties_to_set:
            cursor.execute(property)
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()   
        self.connect.close()
        return result

    def check_mode(self, line, mode='PIPES_AS_CONCAT'):
        """check palo & mysql"""
        sql = "set session sql_mode = %s" % mode
        LOG.info(L('palo set mode', sql=sql))
        LOG.info(L('palo sql', sql=line))
        p_res = runner.query_palo.do_set_properties_sql(line, [sql])
        LOG.info(L('mysql set mode', sql=sql))
        LOG.info(L('mysql sql', sql=line))
        m_res = self.do_sql_mysql(line, [sql])
        util.check_same(p_res, m_res)

    def check2_diff(self, psql, msql, diff=0.1, accurate_column=[], force_order=False):
        """check palo and mysql execute sql same,allow diff percenty is 0.1
        """
        print(psql)
        print(msql)
        times = 0
        flag = 0
        while times <= 10 and flag == 0:
            try:
                line = "set session sql_mode = PIPES_AS_CONCAT"
                LOG.info(L('palo sql', palo_sql=psql))
                palo_result = self.query_palo.do_set_properties_sql(psql, [line])
                LOG.info(L('mysql sql', mysql_sql=msql))
                self.mysql_cursor.execute(msql)
                mysql_result = self.mysql_cursor.fetchall()
                ###actucal vs estimate diff in 1%
                same, gap = util.check_percent_diff(palo_result, mysql_result, accurate_column, force_order, diff)
                #print("flag %s, gap %s" % (same, gap))
                if same == False:
                    assert 0 == 1, "actual max diff %s, excepted diff count %s"\
                        % (gap, diff)
                flag = 1
            except Exception as e:
                print(Exception, ":", e)
                print("hello")
                LOG.error(L('error', error=e))
                time.sleep(1)
                times += 1
                if times == 3:
                    assert 0 == 1


def test_sql_mode_set():
    """
    {
    "title": "test_query_sql_mode.test_sql_mode_set",
    "describe": "test_sql_mode_set",
    "tag": "function,p1,fuzz"
    }
    """
    """test_sql_mode_set"""
    sql_mode_list = ['PIPES_AS_concat', 'pipes_AS_concat', 'pipes_as_concat',\
                     '   pipes_as_concat   ', "' '"]
    sql = "select @@sql_mode"
    for mode in sql_mode_list:
        runner.check_mode(sql, mode)
    line = "set session sql_mode = 'PIPES_AS'"
    runner.checkwrong(line)

    line = 'set sql_mode = concat(@@sql_mode, "STRICT_TRANS_TABLES");'
    msg = "Set statement does\\'t support computing expr"
    runner.checkwrong(line, msg)

    line = 'set sql_mode = "STRICT_TRANS_TABLES";'
    runner.checkok(line)
    line = "set sql_mode='PIPES_AS_CONCAT'"
    runner.check(line)
    

def test_sql_mode_operator():
    """
    {
    "title": "test_query_sql_mode.test_sql_mode_operator",
    "describe": "test for operator",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test for operator
    """
    line = "select  1 || 1, 0 || 1 ||2"
    runner.check_mode(line)
    line = "select  (1+1>0) || 1, (1||1>0) || 1 ||2, upper(('true'||1|| '>b'))"
    runner.check_mode(line)
    line = "select 'a' || NULL,  'a' || 'NULL'"
    runner.check_mode(line)

    line = "select (0-1 || -2 > -2), (-1 || -2 > 0), \
        (8 || -2 > -2), (8-3 || -2 > -2),  select (-1 || -2 > 0)"
    ##bug,todo,issue信息：https://github.com/apache/incubator-doris/issues/2399
    #runner.check(line)

    sql = "select @@sql_mode"
    runner.check_mode(sql)
    line = "select length(('true'||1|| '>b'))"
    runner.check_mode(line)
    line = "select ltrim((\"  \"|| k6|| k7 ||\"  \")) from baseall order by k1, k2, k3, k4"
    runner.check_mode(line)
    line = "select k1,k2,(k1 || k2 >1 and 2<1) from baseall order by k1,k2"
    runner.check_mode(line)
    line = "CREATE VIEW new_view1 as select k1,k2,(k1 || k2 >1 and 2<1) from baseall order by k1,k2"
    runner.check_mode(line)
    line = "select * from new_view1 "
    runner.check_mode(line)
    line = "drop view new_view1"
    runner.check_mode(line)

    sql = "set session sql_mode = PIPES_AS_concat ;"

    line = sql + "select   (1||1>0) || 1 ||'a'||"
    runner.checkwrong(line)

    line = sql + "select (true||1>0||'a'>'b')"
    runner.checkwrong(line)
    line = "select k1,k2,(k1 || k2 > 1 or 2>3) from baseall"
    runner.check2_diff(line, line, diff=0)
    line = "select k1,k2,(k1 || k2 >1 and 2>1) from baseall order by k1,k2"
    runner.check2_diff(line, line, diff=0)
    line = "select k1,k2,(k1 || k2 > 1 or 2>3 and (k1 || k2 > 1)) from baseall order by k1,k2"
    runner.check2_diff(line, line, diff=0)


def test_sql_mode_session_and_global():
    """
    {
    "title": "test_query_sql_mode.test_sql_mode_session_and_global",
    "describe": "test_sql_mode_session_and_global",
    "tag": "function,p1"
    }
    """
    line = "set global sql_mode = PIPES_AS_concat "
    runner.check(line)
    runner.get_clients()
    line = "select k1,k2,(k1 || k2 > 1 or 2>3) from baseall"
    runner.check2_diff(line, line)
    line = "select k1,k2,(k1 || k2 >1 and 2<1) from baseall order by k1,k2"
    runner.check(line)

    line = "set global sql_mode = ''"
    runner.check(line)
    runner.get_clients()
    line = "select (true||1>0||'a'>'b')"
    runner.check(line)
    line = "select k1,k2,(k1 || k2 >1 and 2<1) from baseall order by k1,k2"
    runner.check_mode(line)
    line = "select (true||1>0||'a'>'b')"
    runner.check(line)


def teardown_module():
    """
    end 
    """
    print("End")


if __name__ == "__main__":
    import pdb
    #pdb.set_trace()
    setup_module()
    test_sql_mode_set()
    test_sql_mode_operator()
    test_sql_mode_session_and_global()
