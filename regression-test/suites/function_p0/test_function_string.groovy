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

suite("test_function_string") {

    sql """ set enable_nereids_planner=true; """
    sql """ set enable_fallback_to_original_planner=false; """

    sql """
        drop table if exists test_tb_function_space;
    """

    sql """
    CREATE TABLE `test_tb_function_space` (
        `k1` bigint NULL,
        `k2` text NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
        insert into test_tb_function_space values ('-10000000000', a),('-20000000000', a),('-30000000000', a),('-40000000000', a),('-50000000000', a)
    """

    qt_sql """ 
        select length(concat(space(k1), k2)) as k from test_tb_function_space order by k;
    """

    sql """
        drop table if exists test_tb_function_space;
    """

}
