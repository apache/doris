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

suite("char_type_shrink_before_projec") {
    sql """drop table if exists char_type_shrink_before_projec"""
    sql """
        CREATE TABLE `char_type_shrink_before_projec` (
        `pk` int NULL,
        `col_char_50__undef_signed` char(50) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`pk`)
        DISTRIBUTED BY HASH(`pk`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "min_load_replica_num" = "-1",
            "is_being_synced" = "false",
            "storage_medium" = "hdd",
            "storage_format" = "V2",
            "inverted_index_storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false",
            "group_commit_interval_ms" = "10000",
            "group_commit_data_bytes" = "134217728"
        );
    """

    sql """
        insert into char_type_shrink_before_projec values (159, "ABC");
    """

    qt_0 """
        SELECT Concat(t1.col_char_50__undef_signed, '_suffix'),
        Length(Concat(t1.col_char_50__undef_signed, '_suffix'))
        FROM   char_type_shrink_before_projec AS t1
        WHERE  pk = 159 
    """
}