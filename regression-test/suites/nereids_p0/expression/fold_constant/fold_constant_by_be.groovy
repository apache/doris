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

    explain {
         sql "select sleep(sign(1)*100);"
         contains "sleep(100)"
    }

    sql 'set query_timeout=12;'
    qt_sql "select sleep(sign(1)*5);"
    
    explain {
        sql("verbose select substring('123456', 1, 3)")
        contains "varchar(3)"
    }

    sql "drop table if exists table_200_undef_partitions2_keys3_properties4_distributed_by53"
    sql """create table table_200_undef_partitions2_keys3_properties4_distributed_by53 (
                    pk int,
                    col_char_255__undef_signed char(255)  null  ,
                    col_char_100__undef_signed char(100)  null  ,
                    col_char_255__undef_signed_not_null char(255)  not null  ,
                    col_char_100__undef_signed_not_null char(100)  not null  ,
                    col_varchar_255__undef_signed varchar(255)  null  ,
                    col_varchar_255__undef_signed_not_null varchar(255)  not null  ,
                    col_varchar_1000__undef_signed varchar(1000)  null  ,
                    col_varchar_1000__undef_signed_not_null varchar(1000)  not null  ,
                    col_varchar_1001__undef_signed varchar(1001)  null  ,
                    col_varchar_1001__undef_signed_not_null varchar(1001)  not null  
                    ) engine=olap
                    DUPLICATE KEY(pk, col_char_255__undef_signed, col_char_100__undef_signed)
                    distributed by hash(pk) buckets 10
                    properties("replication_num" = "1");"""
    explain {
        sql("select LAST_VALUE(col_char_255__undef_signed_not_null, false) over (partition by " +
                "concat('GkIPbzAZSu', col_char_100__undef_signed), mask('JrqFkEDqeA') " +
                "order by pk rows between unbounded preceding and 6 following) AS col_alias26947 " +
                "from table_200_undef_partitions2_keys3_properties4_distributed_by53;")
        notContains("mask")
    }
}
