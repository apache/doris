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

suite("test_str_to_date") {
    sql """ DROP TABLE IF EXISTS test_str_to_date_db """

    sql """ 
        CREATE TABLE IF NOT EXISTS test_str_to_date_db (
              `id` INT NULL COMMENT "",
              `s1` String NULL COMMENT "",
              `s2` String NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            );
    """

    sql """ INSERT INTO test_str_to_date_db VALUES(1,'2019-12-01', 'yyyy-MM-dd');"""
    sql """ INSERT INTO test_str_to_date_db VALUES(2,'20201203', 'yyyyMMdd');"""
    sql """ INSERT INTO test_str_to_date_db VALUES(3,'2020-12-03 11:45:14', 'yyyy-MM-dd HH:mm:ss');"""

    qt_select1 """
        select s1,s2,STR_TO_DATE(s1,s2) from test_str_to_date_db order by id;
    """
    qt_select2 """
        SELECT STR_TO_DATE('2019-12-01', 'yyyy-MM-dd');  
    """
    qt_select3 """
        SELECT STR_TO_DATE('20201203', 'yyyyMMdd');
    """
    qt_select4 """
        SELECT STR_TO_DATE('2020-12-03 11:45:14', 'yyyy-MM-dd HH:mm:ss');
    """
    qt_short_1 " select STR_TO_DATE('2023', '%Y') "
    qt_short_2 " select STR_TO_DATE('2023-12', '%Y-%m') "
    qt_short_3 " select STR_TO_DATE('2023-12', '%Y')"
    qt_short_4 " select STR_TO_DATE('2020%2', '%Y%%%m')"
}
