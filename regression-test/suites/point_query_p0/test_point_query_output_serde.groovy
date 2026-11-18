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

suite("test_point_query_output_serde", "p0,nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_point_query_output_serde"
    sql """CREATE TABLE test_point_query_output_serde (
        k BIGINT NOT NULL, n BIGINT NULL, d DOUBLE NULL, s STRING NULL,
        decval DECIMAL(18,2) NULL, dt DATE NULL, a ARRAY<INT> NULL
    ) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 3
    PROPERTIES ('replication_num'='1', 'enable_unique_key_merge_on_write'='true',
                'store_row_column'='true', 'light_schema_change'='true')"""
    sql """INSERT INTO test_point_query_output_serde VALUES
        (1, NULL, NULL, NULL, 12.34, '2024-01-02', [1,NULL,3]),
        (2, -9223372036854775808, -1.23456789e100, repeat('长x', 8000), -99.25, '2024-02-29', []),
        (3, 9223372036854775807, 0.0, '', NULL, NULL, NULL)"""
    def database = sql("SELECT DATABASE()")[0][0]
    def baseUrl = context.config.jdbcUrl
    def url = baseUrl + (baseUrl.contains('?') ? '&' : '?') +
            'useServerPrepStmts=true&emulateUnsupportedPstmts=false'
    for (boolean batch : [false, true]) {
        for (boolean lightweight : [false, true]) {
            setFeConfigTemporary([enable_point_query_rpc_batch: batch,
                                  enable_lightweight_lookup_request: lightweight]) {
                connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
                    sql "USE ${database}"
                    def collect = { rs ->
                        def rows = []
                        try {
                            int cols = rs.metaData.columnCount
                            while (rs.next()) {
                                rows.add((1..cols).collect { rs.getString(it) })
                            }
                        } finally { rs.close() }
                        rows
                    }
                    for (def projection : ['k,n,d,s', 'k,s,n,s,d,k',
                                           'k,decval,dt,a', 'k,cast(n as string),length(s),n-0']) {
                        def query = "SELECT ${projection} FROM test_point_query_output_serde WHERE k = ?"
                        def stmt = prepareStatement(query)
                        try {
                            assertTrue(stmt.class.name.contains('ServerPreparedStatement'))
                            for (long key : [999, 1, 2, 3, 1, 0, -1, Long.MAX_VALUE]) {
                                stmt.setLong(1, key)
                                def actual = collect(stmt.executeQuery())
                                def text = context.getConnection().createStatement()
                                def expected
                                try {
                                    expected = collect(text.executeQuery(query.replace('?', key.toString())))
                                } finally { text.close() }
                                assertEquals(expected, actual)
                            }
                        } finally { stmt.close() }
                    }
                    def live = prepareStatement("SELECT k,n,d,s FROM test_point_query_output_serde WHERE k = ?")
                    try {
                        live.setLong(1, 1)
                        collect(live.executeQuery())
                        sql "UPDATE test_point_query_output_serde SET n=42, d=0.125, s='updated' WHERE k=1"
                        quickExecute("updated_${batch}_${lightweight}", live)
                        sql "DELETE FROM test_point_query_output_serde WHERE k=1"
                        quickExecute("deleted_${batch}_${lightweight}", live)
                        sql "INSERT INTO test_point_query_output_serde(k,n,d,s) VALUES (1,43,0.25,'reinserted')"
                        quickExecute("reinserted_${batch}_${lightweight}", live)
                    } finally { live.close() }
                }
            }
        }
    }
    // A live prepared statement must survive schema/context rebuilding.
    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        sql "USE ${database}"
        def stmt = prepareStatement("SELECT k,s,n,s,d FROM test_point_query_output_serde WHERE k = ?")
        try {
            stmt.setLong(1, 1)
            quickExecute("before_schema_change", stmt)
            sql "ALTER TABLE test_point_query_output_serde ADD COLUMN extra STRING NULL DEFAULT 'new-default'"
            // ADD COLUMN is light schema change on this table.
            quickExecute("after_schema_change", stmt)
            def added = prepareStatement("SELECT k,extra FROM test_point_query_output_serde WHERE k = ?")
            try {
                added.setLong(1, 1)
                quickExecute("new_column", added)
            } finally { added.close() }
        } finally { stmt.close() }
    }
}
