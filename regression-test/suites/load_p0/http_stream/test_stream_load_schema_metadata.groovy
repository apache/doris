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

suite("test_stream_load_schema_metadata", "p0") {
    sql "DROP TABLE IF EXISTS test_stream_load_schema_metadata"
    sql """
        CREATE TABLE test_stream_load_schema_metadata (
            id INT NOT NULL,
            value INT NOT NULL
        ) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "false"
        )
    """

    def db = context.config.getDbNameByFile(context.file)
    def loadRows = { boolean http, boolean groupCommit ->
        streamLoad {
            if (http) {
                set 'version', '1'
                set 'sql', """
                    INSERT INTO ${db}.test_stream_load_schema_metadata (id, value)
                    SELECT c1, c2 FROM http_stream("format"="csv", "column_separator"=",")
                """
            } else {
                table 'test_stream_load_schema_metadata'
                set 'column_separator', ','
                set 'columns', 'id,value'
            }
            if (groupCommit) {
                set 'group_commit', 'sync_mode'
                unset 'label'
            }
            inputStream new ByteArrayInputStream("1,10\n2,20\n".getBytes("UTF-8"))
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(2, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                if (groupCommit) {
                    assertTrue(json.GroupCommit)
                }
            }
        }
    }

    loadRows(false, false)
    loadRows(true, false)
    order_qt_before "SELECT * FROM test_stream_load_schema_metadata"

    // Repeated loads exercise both metadata response paths while the base index is replaced.
    sql "ALTER TABLE test_stream_load_schema_metadata MODIFY COLUMN value BIGINT NOT NULL"
    for (int i = 0; i < 5; i++) {
        loadRows(false, false)
        loadRows(true, false)
    }
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName='test_stream_load_schema_metadata'
               ORDER BY CreateTime DESC LIMIT 1"""
        time 120
    }
    loadRows(false, false)
    loadRows(true, false)
    order_qt_after_replacement "SELECT * FROM test_stream_load_schema_metadata"

    sql "ALTER TABLE test_stream_load_schema_metadata ADD COLUMN added INT DEFAULT '7'"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName='test_stream_load_schema_metadata'
               ORDER BY CreateTime DESC LIMIT 1"""
        time 120
    }
    loadRows(false, false)
    loadRows(true, false)
    loadRows(true, true)
    sql "sync"
    order_qt_after_addition "SELECT * FROM test_stream_load_schema_metadata"
}
