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

suite("test_file_type_resource") {
    // These endpoints are deliberately unavailable: TO_FILE with NULL must not create a remote client or do I/O.
    sql "DROP RESOURCE IF EXISTS 'file_type_no_io_s3'"
    sql "DROP RESOURCE IF EXISTS 'file_type_no_io_hdfs'"
    sql """
        CREATE RESOURCE 'file_type_no_io_s3' PROPERTIES(
            "type"="s3", "AWS_ENDPOINT"="http://127.0.0.1:1", "AWS_REGION"="us-east-1",
            "AWS_BUCKET"="file-type-no-io", "AWS_ROOT_PATH"="files",
            "AWS_ACCESS_KEY"="unused", "AWS_SECRET_KEY"="unused",
            "AWS_REQUEST_TIMEOUT_MS"="200", "AWS_CONNECTION_TIMEOUT_MS"="100",
            "s3_validity_check"="false")
    """
    sql """
        CREATE RESOURCE 'file_type_no_io_hdfs' PROPERTIES(
            "type"="hdfs", "fs.defaultFS"="hdfs://127.0.0.1:1",
            "hadoop.username"="file_type_test", "ipc.client.connect.timeout"="100",
            "ipc.client.connect.max.retries"="0")
    """
    qt_constant_null """
        SELECT TO_FILE('file_type_no_io_s3', NULL), TO_FILE('file_type_no_io_hdfs', NULL)
    """
    sql "DROP TABLE IF EXISTS test_file_type_null_uris"
    sql """
        CREATE TABLE test_file_type_null_uris (id INT NOT NULL, uri STRING NULL)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES("replication_num"="1")
    """
    sql "INSERT INTO test_file_type_null_uris VALUES (1, NULL), (2, NULL)"
    qt_column_null """
        SELECT id, TO_FILE('file_type_no_io_s3', uri), TO_FILE('file_type_no_io_hdfs', uri),
               ELEMENT_AT(TO_FILE('file_type_no_io_s3', uri), 'uri')
        FROM test_file_type_null_uris ORDER BY id
    """
    for (def query in [
        "SELECT TO_FILE(NULL, NULL)",
        "SELECT TO_FILE('file_type_no_io_s3', 123)",
        "SELECT TO_FILE(uri, NULL) FROM test_file_type_null_uris"
    ]) {
        test {
            sql query
            exception "TO_FILE"
        }
    }
}
