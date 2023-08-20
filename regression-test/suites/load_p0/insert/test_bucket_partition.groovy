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

suite("test_bucket_partition") {
    def insert_tbl = "test_insert_b_p";

    sql """ DROP TABLE IF EXISTS ${insert_tbl}"""

    sql """
        CREATE TABLE ${insert_tbl} (
            `k2` varchar(10) default "10",
        ) ENGINE=OLAP
        DUPLICATE KEY(`k2`)
        COMMENT 'OLAP'
        PARTITION BY LIST(k2)
        PARTITION BY RANGE(col) (
            PARTITION `p_cn` VALUES IN ("10"),
            PARTITION `p_cn` VALUES IN ("0")
        )
        DISTRIBUTED BY HASH(`k2`) BUCKETS 1
        PROPERTIES (
            "replication_num"="1"
        );
    """

    sql """ set enable_nereids_planner=true """
    sql """ set enable_nereids_dml=true """
    sql """ insert into ${insert_tbl} values() """

    sql """ set enable_nereids_planner=false """
    sql """ set enable_nereids_dml=false """
    sql """ insert into ${insert_tbl} values() """
    
    qt_select """ select k2 from ${insert_tbl} """
}
