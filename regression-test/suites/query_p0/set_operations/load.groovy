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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/window_functions
// and modified by Doris.
suite("load") {
    def tables = ['lineitem', 'region', 'nation', 'part', 'supplier', 'partsupp', 'workers']

    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS tpch_tiny_${table} """
    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }
    
    for (String tableName in tables) {
        streamLoad {
            // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table "tpch_tiny_${tableName}"

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url() + '/regression/tpch/sf0.01/' + tableName}.csv.gz"""

            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals('success', json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    // nested array with join
    def test_nested_array_2_depths = {
        def tableName = "nested_array_test_2_vectorized"

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `key` INT,
                value ARRAY<ARRAY<INT>>
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """

        sql "INSERT INTO ${tableName} VALUES (1, [])"
        sql "INSERT INTO ${tableName} VALUES (2, [null])"
        sql "INSERT INTO ${tableName} VALUES (3, [[]])"
        sql "INSERT INTO ${tableName} VALUES (4, [[1, 2, 3], [4, 5, 6]])"
        sql "INSERT INTO ${tableName} VALUES (5, [[1, 2, 3], null, [4, 5, 6]])"
        sql "INSERT INTO ${tableName} VALUES (6, [[1, 2, null], null, [4, null, 6], null, [null, 8, 9]])"
       
        sql """
            INSERT INTO ${tableName} VALUES
                (1, []),
                (2, [null]),
                (3, [[]]),
                (4, [[1, 2, 3], [4, 5, 6]]),
                (5, [[1, 2, 3], null, [4, 5, 6]]),
                (6, [[1, 2, null], null, [4, null, 6], null, [null, 8, 9]])
        """ 

         sql """
            INSERT INTO ${tableName} VALUES
                (1, []),
                (2, [null]),
                (3, [[]]),
                (4, [[1, 2, 3], [4, 5, 6]]),
                (5, [[1, 2, 3], null, [4, 5, 6]]),
                (6, [[1, 2, null], null, [4, null, 6], null, [null, 8, 9]])
        """ 
    }
    test_nested_array_2_depths.call()
    }
