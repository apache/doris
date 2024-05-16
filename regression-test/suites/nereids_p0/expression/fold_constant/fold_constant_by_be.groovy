// Licensed to the Apache Software Foundation (ASF) under one
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

suite("fold_constant_by_be") {
    sql 'use nereids_fold_constant_test'

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_fold_constant_by_be=true'

    test {
        sql '''
            select if(
                date_format(CONCAT_WS('', '9999-07', '-26'), '%Y-%m') = DATE_FORMAT(curdate(), '%Y-%m'),
                curdate(),
                DATE_FORMAT(DATE_SUB(month_ceil(CONCAT_WS('', '9999-07', '-26')), 1), '%Y-%m-%d'))
        '''
        result([['9999-07-31']])
    }

    sql """ 
        CREATE TABLE IF NOT EXISTS str_tb (k1 VARCHAR(10) NULL, v1 STRING NULL) 
        UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
    """

    sql """ INSERT INTO str_tb VALUES (2, repeat("test1111", 10000)); """

    qt_sql_1 """ select length(v1) from str_tb; """

    def res1 = sql " select /*+SET_VAR(enable_fold_constant_by_be=true)*/ ST_CIRCLE(121.510651, 31.234391, 1918.0); "
    def res2 = sql " select /*+SET_VAR(enable_fold_constant_by_be=false)*/ ST_CIRCLE(121.510651, 31.234391, 1918.0); "
    log.info("result: {}, {}", res1, res2)
    assertEquals(res1[0][0], res2[0][0])
}