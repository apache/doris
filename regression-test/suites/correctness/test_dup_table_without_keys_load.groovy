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

suite("test_dup_table_without_keys_load") {

    sql """ DROP TABLE IF EXISTS test_dup_table_without_keys_load """
    sql """
            CREATE TABLE IF NOT EXISTS test_dup_table_without_keys_load (
              `user_id` bigint(20) NULL,
              `is_delete` tinyint(4) NULL,
              `client_version_int` int(11) NULL
            ) ENGINE=OLAP
            COMMENT 'duplicate_no_keys'
            DISTRIBUTED BY HASH(`user_id`) BUCKETS 4
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            );
            """

    sql """ INSERT INTO test_dup_table_without_keys_load VALUES 
            (1,1,1),
            (2,0,2),
            (3,1,3),
            (4,0,4),
            (5,1,5),
            (6,1,1),
            (7,0,2),
            (8,1,3),
            (9,0,4),
            (10,1,5);
            """

    sql """ INSERT INTO test_dup_table_without_keys_load VALUES 
            (11,1,1),
            (12,0,2),
            (13,1,3),
            (14,0,4),
            (15,1,5),
            (16,1,1),
            (17,0,2),
            (18,1,3),
            (19,0,4),
            (20,1,5);
            """

    test {
        sql """
            SELECT * FROM test_dup_table_without_keys_load;
        """
        rowNum 20
    }

    sql """ DROP TABLE IF EXISTS test_dup_table_without_keys_load """
    sql """
            CREATE TABLE IF NOT EXISTS test_dup_table_without_keys_load (
              `user_id` bigint(20) NULL,
              `is_delete` tinyint(4) NULL,
              `client_version_int` int(11) NULL
            ) ENGINE=OLAP
            COMMENT 'duplicate_no_keys'
            DISTRIBUTED BY HASH(`user_id`) BUCKETS 4
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            );
            """

    streamLoad {
        // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
        // db 'regression_test'
        db 'regression_test_correctness'
        table 'test_dup_table_without_keys_load'

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', ','

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'test_dup_table_without_keys_load.csv'

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows
    }

    sql """sync"""
    test {
        sql """
            SELECT * FROM test_dup_table_without_keys_load;
        """
        rowNum 20
    }
}
