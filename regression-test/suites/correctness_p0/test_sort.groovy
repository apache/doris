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

suite("test_sort") {

    sql """
        DROP TABLE IF EXISTS test_sort_table 
    """

    sql """
        CREATE TABLE `test_sort_table` (
          `p_partkey` int(11) NOT NULL COMMENT "",
          `p_name` varchar(23) NOT NULL COMMENT "",
          `p_mfgr` varchar(7) NOT NULL COMMENT "",
          `p_category` varchar(8) NOT NULL COMMENT "",
          `p_brand` varchar(10) NOT NULL COMMENT "",
          `p_color` varchar(12) NOT NULL COMMENT "",
          `p_type` varchar(26) NOT NULL COMMENT "",
          `p_size` int(11) NOT NULL COMMENT "",
          `p_container` varchar(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`p_partkey`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "colocate_with" = "groupa5",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
    """

    sql """
        INSERT INTO test_sort_table VALUES (1, '1', '1', '1', '1', '1', '1', 1, '1');
    """

    qt_sql """
        select count(1) from (select* from test_sort_table where p_partkey = 1 order by p_partkey desc limit 1) a;
    """

    sql "DROP TABLE test_sort_table"

    qt_sql """
        select b.k1, a.k1, b.k4, a.k4 from test_query_db.baseall a left join test_query_db.test b on a.k2 = b.k4  order by 1, 2, 3, 4;
    """
}
