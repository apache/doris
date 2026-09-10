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

suite("test_stream_load_chunked_orc", "p0") {
    sql """DROP TABLE IF EXISTS test_stream_load_chunked_orc"""
    sql """
        CREATE TABLE test_stream_load_chunked_orc (
            id INT,
            decimal_col1 DECIMALV3(8, 4),
            decimal_col2 DECIMALV3(18, 6),
            decimal_col3 DECIMALV3(38, 12),
            decimal_col4 DECIMALV3(9, 0),
            decimal_col5 DECIMAL(27, 9),
            decimal_col6 DECIMAL(9, 0)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    streamLoad {
        table "test_stream_load_chunked_orc"
        set "format", "orc"
        // InputStreamEntity has no known content length, so HttpClient uses chunked transfer.
        inputStream new FileInputStream("${context.dataPath}/test_decimal.orc")
        time 10000
    }

    order_qt_chunked_orc """SELECT * FROM test_stream_load_chunked_orc"""
}
