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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("test_schema_change_delete") {
    def tableName = "sc_delete_tbl"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName} (
          `col1` tinyint NOT NULL,
          `col2` varchar(11) NOT NULL,
          `col3` date NOT NULL,
          `col5` int REPLACE NOT NULL,
          `col4` date REPLACE NOT NULL,
          `col6` float REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`col1`, `col2`, `col3`)
        DISTRIBUTED BY HASH(`col1`, `col2`, `col3`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "true",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728"
        )
    """

    sql """ insert into ${tableName} values (-100, '', '2023-10-19', -72, '2024-01-01', '1.1'); """

    sql """ delete from ${tableName} where col1 = -100 and col2 = '' and col3 = '2023-10-19' """

    sql """ alter table ${tableName} MODIFY COLUMN `col5` string REPLACE NOT NULL """

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    qt_select "select * from ${tableName}"
}
