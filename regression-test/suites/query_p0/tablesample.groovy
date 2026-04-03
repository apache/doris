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

suite("test_table_sample") {
    sql """DROP TABLE IF EXISTS test_table_sample_tbl"""
    sql """
        CREATE TABLE `test_table_sample_tbl` (
          `col1` varchar(11451) NOT NULL,
          `col2` int(11) NOT NULL,
          `col3` int(11) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`col1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`col1`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        ); 
    """

    // result is random, couldn't check it
    sql """ select * from test_table_sample_tbl tablesample(4 rows);"""
    sql """select * from test_table_sample_tbl t tablesample(20 percent);"""
    sql """select * from test_table_sample_tbl t tablesample(20 percent) repeatable 2;"""
}
