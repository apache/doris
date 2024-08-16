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


suite("test_condition_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set enable_fold_constant_by_be=false;"
    sql "set disable_nereids_rules='prune_empty_partition'"

    sql "drop table if exists test_condition_function_partition_prune;"

    sql """CREATE TABLE `test_condition_function_partition_prune` (id int ,`period` TEXT NULL, `part_dt` datetime NULL )
        PARTITION BY RANGE(`part_dt`)
        (PARTITION p20240501 VALUES [('2024-05-01 00:00:00'), ('2024-05-02 00:00:00'))
        , PARTITION p20240601 VALUES [('2024-06-01 00:00:00'), ('2024-06-02 00:00:00'))
        , PARTITION p20240610 VALUES [('2024-06-10 00:00:00'), ('2024-06-11 00:00:00')))
        distributed by hash(id) properties("replication_num"="1");"""

    sql "insert into test_condition_function_partition_prune(id,part_dt,period) values(1,'2024-05-01','20240606');"
    sql "insert into test_condition_function_partition_prune(id,part_dt,period) values(2,'2024-06-01','20240606');"
    sql "insert into test_condition_function_partition_prune(id,part_dt,period) values(3,'2024-06-10','20240606');"

    explain {
        sql """
        select period
        from test_condition_function_partition_prune as a
        where
        case when substr('abc',1,2)<>'123'
        then substr(part_dt,1,4)>=substr('20240609',1,4)
        else
        part_dt<'20240601' and part_dt>='20240101'
        end;"""
        contains "partitions=3/3 (p20240501,p20240601,p20240610)"
    }

}