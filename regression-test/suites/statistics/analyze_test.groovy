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

suite("analyze_test") {
    sql """
        DROP TABLE IF EXISTS test_table_alter_column_stats
    """
    sql """CREATE TABLE test_table_alter_column_stats (col1 varchar(11451) not null, col2 int not null, col3 int not null)
    UNIQUE KEY(col1)
    DISTRIBUTED BY HASH(col1)
    BUCKETS 3
    PROPERTIES(
            "replication_num"="1",
            "enable_unique_key_merge_on_write"="true"
    );"""


    sql """insert into test_table_alter_column_stats values(1, 2, 3);"""
    sql """insert into test_table_alter_column_stats values(4, 5, 6);"""
    sql """insert into test_table_alter_column_stats values(7, 1, 9);"""
    sql """insert into test_table_alter_column_stats values(3, 8, 2);"""
    sql """insert into test_table_alter_column_stats values(5, 2, 1);"""

    sql """delete from __internal_schema.column_statistics where col_id in  ('col1', 'col2', 'col3')"""

    sql """
        analyze sync table test_table_alter_column_stats;
    """

    order_qt_sql """
        select count, ndv, null_count, min, max, data_size_in_bytes from __internal_schema.column_statistics where
            col_id in ('col1', 'col2', 'col3') order by col_id
    """
    sql """
        DROP TABLE IF EXISTS test_table_alter_column_stats
    """
}