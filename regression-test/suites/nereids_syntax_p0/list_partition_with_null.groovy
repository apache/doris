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

suite("list_partition_with_null") {
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_nereids_planner=true"

    sql """DROP TABLE IF EXISTS table_null_list_parition;"""

    sql """
        CREATE TABLE table_null_list_parition
        (
            `USER_ID` LARGEINT  COMMENT "用户ID",
            `CITY` VARCHAR(20)  COMMENT "用户所在城市"
        )
        ENGINE=OLAP
        DUPLICATE KEY(`USER_ID`, `CITY`)
        PARTITION BY LIST(`USER_ID`, `CITY`)
        (
            PARTITION `P1_CITY` VALUES IN (("1", null), ("1", "SHANGHAI")),
            PARTITION `P2_CITY` VALUES IN (("2", "BEIJING"), (null, "SHANGHAI")),
            PARTITION `P3_CITY` VALUES IN ((null,null), ("3", "SHANGHAI"))
        )
        DISTRIBUTED BY HASH(`USER_ID`) BUCKETS 16
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """
    sql """insert into table_null_list_parition values(1,null),(null,null),(null,'SHANGHAI');"""
    explain {
            sql("select * from table_null_list_parition where city is null;")
            verbose true
            contains("partitions=2/3 (P1_CITY,P3_CITY)")
    }
    
    explain {
            sql("select * from table_null_list_parition where user_id is null;")
            verbose true
            contains("partitions=2/3 (P2_CITY,P3_CITY)")
    }
    explain {
            sql("select * from table_null_list_parition where city is not null;")
            verbose true
            contains("partitions=3/3 (P1_CITY,P2_CITY,P3_CITY)")
    }
    
    explain {
            sql("select * from table_null_list_parition where user_id is not null;")
            verbose true
            contains("partitions=3/3 (P1_CITY,P2_CITY,P3_CITY)")
    }
    qt_select1 """select city from table_null_list_parition where city is null order by city;"""
    qt_select2 """select user_id from table_null_list_parition where user_id is null;"""
    qt_select3 """select city from table_null_list_parition where city is not null;"""
    qt_select4 """select user_id from table_null_list_parition where user_id is not null;"""
}
