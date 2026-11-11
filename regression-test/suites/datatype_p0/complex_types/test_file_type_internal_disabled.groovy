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
    sql "DROP RESOURCE IF EXISTS 'file_ddl_resource'"
    sql """
        CREATE RESOURCE 'file_ddl_resource' PROPERTIES(
            "type"="s3", "AWS_ENDPOINT"="http://127.0.0.1:1", "AWS_REGION"="us-east-1",
            "AWS_BUCKET"="file-test", "AWS_ACCESS_KEY"="fake", "AWS_SECRET_KEY"="fake",
            "s3_validity_check"="false")
    """
    try {
        def file = "TO_FILE('file_ddl_resource', NULL)"
        def fileValues = [
                "FILE": file,
                "ARRAY<FILE>": "ARRAY(${file})",
                "STRUCT<f:FILE>": "NAMED_STRUCT('f', ${file})",
                "MAP<STRING,FILE>": "MAP('f', ${file})",
                "ARRAY<STRUCT<f:FILE>>": "ARRAY(NAMED_STRUCT('f', ${file}))",
                "STRUCT<f:MAP<STRING,ARRAY<FILE>>>": "NAMED_STRUCT('f', MAP('f', ARRAY(${file})))"]
        for (def type : fileValues.keySet()) {
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
                    CREATE TABLE test_file_type_disabled_ctas
                    DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")
                    AS SELECT 1 AS id, ${fileValues[type]} AS f
                """
                exception "FILE and types containing FILE cannot be used as table columns"
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
        for (def type : ["FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>"]) {
            test {
                sql "ALTER TABLE test_file_type_disabled_base ADD COLUMN added ${type}"
                exception "FILE and types containing FILE cannot be used as table columns"
            }
            test {
                sql "ALTER TABLE test_file_type_disabled_base MODIFY COLUMN f ${type}"
                exception "FILE and types containing FILE cannot be used as table columns"
            }
        }
        for (def type : ["FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>"]) {
            for (def distributionColumn : ["id", "f"]) {
                test {
                    sql """
                        CREATE MATERIALIZED VIEW test_file_type_disabled_mv
                        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
                        DISTRIBUTED BY HASH(${distributionColumn}) BUCKETS 1
                        PROPERTIES("replication_num"="1", "enable_nondeterministic_function"="true")
                        AS SELECT id, ${fileValues[type]} AS f FROM test_file_type_disabled_base
                    """
                    exception "FILE and types containing FILE cannot be used as table columns"
                }
            }
        }
        qt_count "SELECT COUNT(f) FROM (SELECT ${file} AS f) t"
        sql "DROP TABLE IF EXISTS test_file_type_disabled_scalar"
        sql """
            CREATE TABLE test_file_type_disabled_scalar
            DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")
            AS SELECT 1 AS id, ELEMENT_AT(f, 'uri') AS uri,
                      HEX(CAST(FL_GET_BINARY(f) AS STRING)) AS inline_hex
            FROM (SELECT ${file} AS f) t
        """
        qt_scalar "SELECT id, uri, inline_hex FROM test_file_type_disabled_scalar ORDER BY id"

    } finally {
        sql "DROP RESOURCE IF EXISTS 'file_ddl_resource'"
    }
}
