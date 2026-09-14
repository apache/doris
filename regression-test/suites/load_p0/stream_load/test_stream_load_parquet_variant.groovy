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

// Stream loads a Parquet file whose column uses the Parquet VARIANT logical type
// (written by Doris with "parquet.variant_encoding" = "variant") into a Variant V2 column.
suite("test_stream_load_parquet_variant", "p0") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "DROP TABLE IF EXISTS test_stream_load_parquet_variant"
        sql """
            CREATE TABLE test_stream_load_parquet_variant (
                id INT NOT NULL,
                v VARIANT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        streamLoad {
            table "test_stream_load_parquet_variant"
            set 'format', 'parquet'
            file 'test_parquet_variant_encoding.parquet'
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(9, json.NumberTotalRows)
                assertEquals(9, json.NumberLoadedRows)
            }
        }
        sql "sync"
        qt_select """
            select id, v, cast(v['name'] as string) as name, cast(v['nested']['zip'] as int) as zip,
                   variant_type(v) as type
            from test_stream_load_parquet_variant order by id
        """
    }
}
