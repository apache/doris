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

suite("test_http_stream_v2_schema", "p0") {
    sql "DROP TABLE IF EXISTS test_http_stream_v2_schema"
    sql """
        CREATE TABLE test_http_stream_v2_schema (
            kind VARCHAR(32),
            id INT,
            name VARCHAR(32)
        ) DUPLICATE KEY(kind, id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    def db = context.config.getDbNameByFile(context.file)
    def cases = [
        [kind: 'csv', format: 'csv', fields: 'c1, c2',
         data: '1,first\n2,second\n', options: ''],
        [kind: 'csv_names', format: 'csv_with_names', fields: 'id, name',
         data: 'id,name\n1,first\n2,second\n', options: ''],
        [kind: 'csv_types', format: 'csv_with_names_and_types', fields: 'id, name',
         data: 'id,name\nINT,STRING\n1,first\n2,second\n', options: ''],
        [kind: 'text', format: 'hive_text', fields: 'c1, c2',
         data: '1,first\n2,second\n', options: ''],
        [kind: 'json_lines', format: 'json', fields: 'id, name',
         data: '{"id":1,"name":"first"}\n{"id":2,"name":"second"}\n',
         options: ', "read_json_by_line"="true"'],
        [kind: 'json_paths', format: 'json', fields: 'id, name',
         data: '{"rows":[{"id":1,"nested":{"name":"first"}},{"id":2,"nested":{"name":"second"}}]}',
         options: ', "strip_outer_array"="true", "json_root"="$.rows", "jsonpaths"=\'["$.id","$.nested.name"]\'']
    ]
    for (def item : cases) {
        streamLoad {
            set 'version', '1'
            set 'sql', """
                INSERT INTO ${db}.test_http_stream_v2_schema
                SELECT '${item.kind}', ${item.fields}
                FROM http_stream("format"="${item.format}", "column_separator"="," ${item.options})
            """
            inputText item.data
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def response = parseJson(result)
                assertEquals("success", response.Status.toLowerCase())
                assertEquals(2, response.NumberLoadedRows)
                assertEquals(0, response.NumberFilteredRows)
            }
        }
    }
    sql "sync"
    order_qt_schemas "SELECT * FROM test_http_stream_v2_schema ORDER BY kind, id"
}
