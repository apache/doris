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

suite("test_file_type_internal_disabled") {
    def fileTypes = ["FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>",
                     "ARRAY<STRUCT<f:FILE>>", "STRUCT<f:MAP<STRING,ARRAY<FILE>>>"]
    for (def type : fileTypes) {
        for (def model : ["DUPLICATE KEY(id)", "UNIQUE KEY(id)", "AGGREGATE KEY(id)"]) {
            def aggregation = model.startsWith("AGGREGATE") ? " REPLACE" : ""
            test {
                sql """
                    CREATE TABLE test_file_type_disabled (id INT, f ${type}${aggregation})
                    ${model} DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1")
                """
                exception "FILE and types containing FILE cannot be used as table columns"
            }
        }
        test {
            sql """
                CREATE TABLE test_file_type_disabled_hash (id INT, f ${type})
                DUPLICATE KEY(id) DISTRIBUTED BY HASH(f) BUCKETS 1
                PROPERTIES("replication_num"="1")
            """
            exception "FILE and types containing FILE cannot be used as table columns"
        }
    }
    sql "DROP TABLE IF EXISTS test_file_type_disabled_base"
    sql """
        CREATE TABLE test_file_type_disabled_base (id INT, f STRING)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")
    """
    for (def type : fileTypes) {
        test {
            sql "ALTER TABLE test_file_type_disabled_base ADD COLUMN added ${type}"
            exception "FILE and types containing FILE cannot be used as table columns"
        }
        test {
            sql "ALTER TABLE test_file_type_disabled_base MODIFY COLUMN f ${type}"
            exception "FILE and types containing FILE cannot be used as table columns"
        }
    }
}
