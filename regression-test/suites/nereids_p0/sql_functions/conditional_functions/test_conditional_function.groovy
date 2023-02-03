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

suite("test_conditional_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "set batch_size = 4096;"

    def tbName = "test_conditional_function"
    sql "DROP TABLE IF EXISTS ${tbName};"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                user_id INT
            )
            DISTRIBUTED BY HASH(user_id) BUCKETS 5 properties("replication_num" = "1");
        """
    sql """
        INSERT INTO ${tbName} VALUES 
            (1),
            (2),
            (3),
            (4);
        """
    sql """
        INSERT INTO ${tbName} VALUES 
            (null),
            (null),
            (null),
            (null);
        """

    qt_sql "select user_id, case user_id when 1 then 'user_id = 1' when 2 then 'user_id = 2' else 'user_id not exist' end test_case from ${tbName} order by user_id;"
    qt_sql "select user_id, case when user_id = 1 then 'user_id = 1' when user_id = 2 then 'user_id = 2' else 'user_id not exist' end test_case from ${tbName} order by user_id;"

    qt_sql "select user_id, if(user_id = 1, \"true\", \"false\") test_if from ${tbName} order by user_id;"

    qt_sql "select coalesce(NULL, '1111', '0000');"

    qt_sql "select ifnull(1,0);"
    qt_sql "select ifnull(null,10);"
    qt_sql "select ifnull(1,user_id) from ${tbName} order by user_id;"
    qt_sql "select ifnull(user_id,1) from ${tbName} order by user_id;"
    qt_sql "select ifnull(null,user_id) from ${tbName} order by user_id;"
    qt_sql "select ifnull(user_id,null) from ${tbName} order by user_id;"

    qt_sql "select nullif(1,1);"
    qt_sql "select nullif(1,0);"
    qt_sql "select nullif(1,user_id) from ${tbName} order by user_id;"
    qt_sql "select nullif(user_id,1) from ${tbName} order by user_id;"
    qt_sql "select nullif(null,user_id) from ${tbName} order by user_id;"
    qt_sql "select nullif(user_id,null) from ${tbName} order by user_id;"


    qt_sql "select nullif(1,1);"
    qt_sql "select nullif(1,0);"


    qt_sql "select is_null_pred(user_id) from ${tbName} order by user_id"
    qt_sql "select is_not_null_pred(user_id) from ${tbName} order by user_id"

    qt_sql """select if(date_format(CONCAT_WS('', '9999-07', '-26'), '%Y-%m')= DATE_FORMAT( curdate(), '%Y-%m'),
	        curdate(),
	        DATE_FORMAT(DATE_SUB(month_ceil ( CONCAT_WS('', '9999-07', '-26')), 1), '%Y-%m-%d'));"""

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'),3);"

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-01'), '%Y-%m'),3);"

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'),date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'));"

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'),date_format(CONCAT_WS('', '9999-07', '-26'), '%Y-%m'));"

    qt_sql "select ifnull( user_id, to_date('9999-01-01')) r from ${tbName} order by r"

    qt_sql "select ifnull( user_id, 999) r from ${tbName} order by r"

    qt_if_true_then_nullable """select IF(true, DAYOFWEEK("2022-12-06 17:48:46"), 1) + 1;"""
    qt_if_true_else_nullable """select IF(true, 1, DAYOFWEEK("2022-12-06 17:48:46")) + 1;"""

    qt_if_false_then_nullable """select IF(false, DAYOFWEEK("2022-12-06 17:48:46"), 1) + 1;"""
    qt_if_false_else_nullable """select IF(false, 1, DAYOFWEEK("2022-12-06 17:48:46")) + 1;"""


    qt_sql "select user_id, case user_id when 1 then 'user_id = 1' when 2 then 'user_id = 2' else 'user_id not exist' end test_case from ${tbName} order by user_id;"
    qt_sql "select user_id, case when user_id = 1 then 'user_id = 1' when user_id = 2 then 'user_id = 2' else 'user_id not exist' end test_case from ${tbName} order by user_id;"

    qt_sql "select user_id, if(user_id = 1, \"true\", \"false\") test_if from ${tbName} order by user_id;"

    qt_sql "select coalesce(NULL, '1111', '0000');"

    qt_sql "select ifnull(1,0);"
    qt_sql "select ifnull(null,10);"
    qt_sql "select ifnull(1,user_id) from ${tbName} order by user_id;"
    qt_sql "select ifnull(user_id,1) from ${tbName} order by user_id;"
    qt_sql "select ifnull(null,user_id) from ${tbName} order by user_id;"
    qt_sql "select ifnull(user_id,null) from ${tbName} order by user_id;"

    qt_sql "select nullif(1,1);"
    qt_sql "select nullif(1,0);"
    qt_sql "select nullif(1,user_id) from ${tbName} order by user_id;"
    qt_sql "select nullif(user_id,1) from ${tbName} order by user_id;"
    qt_sql "select nullif(null,user_id) from ${tbName} order by user_id;"
    qt_sql "select nullif(user_id,null) from ${tbName} order by user_id;"


    qt_sql "select nullif(1,1);"
    qt_sql "select nullif(1,0);"


    qt_sql "select is_null_pred(user_id) from ${tbName} order by user_id"
    qt_sql "select is_not_null_pred(user_id) from ${tbName} order by user_id"

    qt_sql """select if(date_format(CONCAT_WS('', '9999-07', '-26'), '%Y-%m')= DATE_FORMAT( curdate(), '%Y-%m'),
	        curdate(),
	        DATE_FORMAT(DATE_SUB(month_ceil ( CONCAT_WS('', '9999-07', '-26')), 1), '%Y-%m-%d'));"""

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'),3);"

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-01'), '%Y-%m'),3);"

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'),date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'));"

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'),date_format(CONCAT_WS('', '9999-07', '-26'), '%Y-%m'));"

    qt_sql "select ifnull( user_id, to_date('9999-01-01')) r from ${tbName} order by r"

    qt_sql "select ifnull( user_id, 999) r from ${tbName} order by r"

    qt_if_true_then_nullable """select IF(true, DAYOFWEEK("2022-12-06 17:48:46"), 1) + 1;"""
    qt_if_true_else_nullable """select IF(true, 1, DAYOFWEEK("2022-12-06 17:48:46")) + 1;"""

    qt_if_false_then_nullable """select IF(false, DAYOFWEEK("2022-12-06 17:48:46"), 1) + 1;"""
    qt_if_false_else_nullable """select IF(false, 1, DAYOFWEEK("2022-12-06 17:48:46")) + 1;"""

    qt_sql "select date_add('9999-08-01 00:00:00',1);"

    sql "DROP TABLE ${tbName};"
}
