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

suite("test_partition_sort_operator") {
    def dbName = "test_partition_sort_operator"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE $dbName"
    sql """ DROP TABLE IF EXISTS baseall """
    sql """
       CREATE TABLE IF NOT EXISTS baseall (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
    """
    sql """ set forbid_unknown_col_stats = false """
    streamLoad {
        table "baseall"
        db dbName
        set 'column_separator', ','
        file "../query_p0/baseall.txt"
    }

    sql"""set enable_pipeline_engine = true; """

    qt_pipeline_1 """
        select * from (select k6,k2,row_number() over(partition by k6 order by k2) as num from baseall) as res where num < 5
        ORDER BY 1, 2,3;
    """
    qt_pipeline_2 """
        select * from (select k6,k2,rank() over(partition by k6 order by k2) as num from baseall) as res where num < 5
        ORDER BY 1, 2,3;
    """

    qt_pipeline_3 """
        select * from (select k6,k2,dense_rank() over(partition by k6 order by k2) as num from baseall) as res where num < 5
        ORDER BY 1, 2,3;
    """
    
    sql"""set experimental_enable_pipeline_x_engine=true;    """

    qt_pipelineX_1 """
        select * from (select k6,k2,row_number() over(partition by k6 order by k2) as num from baseall) as res where num < 5
        ORDER BY 1, 2,3;
    """
    qt_pipelineX_2 """
        select * from (select k6,k2,rank() over(partition by k6 order by k2) as num from baseall) as res where num < 5
        ORDER BY 1, 2,3;
    """
    qt_pipelineX_3 """
        select * from (select k6,k2,dense_rank() over(partition by k6 order by k2) as num from baseall) as res where num < 5
        ORDER BY 1, 2,3;
    """


}