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

suite("test_load_with_decimal", "p0,external,hive") {
    def tableName = "test_load_with_decimal"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    /*
      decimal_col1 DECIMALV3(8, 4), // Decimal v3 with 4 bytes
      decimal_col2 DECIMALV3(18, 6), // Decimal v3 with 8 bytes
      decimal_col3 DECIMALV3(38, 12), // Decimal v3 with 16 bytes
      decimal_col4 DECIMALV3(9, 0), // Decimal v3 with default precision and scale
      decimal_col5 DECIMAL(27, 9), // Decimal with max precision and scale
      decimal_col6 DECIMAL(9, 0) // Decimal with default precision and scale
    **/
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
           id INT,
           decimal_col1 DECIMALV3(8, 4), 
           decimal_col2 DECIMALV3(18, 6),
           decimal_col3 DECIMALV3(38, 12),
           decimal_col4 DECIMALV3(9, 0),
           decimal_col5 DECIMAL(27, 9),
           decimal_col6 DECIMAL(9, 0)
        ) ENGINE=OLAP
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES("replication_num" = "1");
    """

    streamLoad {
        table "${tableName}"
        set 'format', 'parquet'
        file 'test_decimal.parquet'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql1 "select * from ${tableName} order by id"
    sql """truncate table ${tableName}"""

    streamLoad {
        table "${tableName}"
        set 'format', 'orc'
        file 'test_decimal.orc'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql2 "select * from ${tableName} order by id"
    sql """truncate table ${tableName}"""

    streamLoad {
        table "${tableName}"
        set 'format', 'csv'
        set 'column_separator', ','
        file 'test_decimal.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql3 "select * from ${tableName} order by id"
    sql """truncate table ${tableName}"""

    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'strip_outer_array', 'true'
        file 'test_decimal.json'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql4 "select * from ${tableName} order by id"
    sql """truncate table ${tableName}"""


    sql """ DROP TABLE IF EXISTS ${tableName} """
}

