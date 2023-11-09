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

suite("test_string_basic") {
    sql """ DROP TABLE IF EXISTS test_str_column_max_len """
    sql """
            CREATE TABLE IF NOT EXISTS `test_str_column_max_len` (
            `k1` tinyint(4) NULL COMMENT "",
            `k2` smallint(6) NULL COMMENT "",
            `k3` int(11) NULL COMMENT "",
            `k4` bigint(20) NULL COMMENT "",
            `k5` decimal(9, 3) NULL COMMENT "",
            `k6` char(5) NULL COMMENT "",
            `k10` date NULL COMMENT "",
            `k11` datetime NULL COMMENT "",
            `k7` varchar(20) NULL COMMENT "",
            `k8` double NULL COMMENT "",
            `k9` float NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
    streamLoad {
        table "test_str_column_max_len"

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', '\t'

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'test_string_column_max_leng.csv'

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }

    sql "sync"

    test {
        sql """
            select
                    /*+ SET_VAR(query_timeout = 600, batch_size=4096) */
                    ref_0.`k6` as c0,
                    coalesce(ref_0.`k9`, ref_0.`k9`) as c1,
                    coalesce(
                            rpad(
                                    cast(ref_0.`k7` as varchar),
                                    cast(ref_0.`k3` as int),
                                    cast(ref_0.`k7` as varchar)
                            ),
                            hex(cast(ref_0.`k7` as varchar))
                    ) as c2,
                    ref_0.`k1` as c3,
                    ref_0.`k2` as c4
            from
                    regression_test_datatype_p0.test_str_column_max_len as ref_0
            where
                    ref_0.`k6` is not NULL;
        """
        exception "string column length is too large"
    }

    sql "drop table if exists fail_tb1"
    // first column could not be string
    test {
        sql """CREATE TABLE IF NOT EXISTS fail_tb1 (k1 STRING NOT NULL, v1 STRING NOT NULL) DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")"""
        exception "The olap table first column could not be float, double, string or array, please use decimal or varchar instead."
    }
    // string type should could not be key
    test {
        sql """
            CREATE TABLE IF NOT EXISTS fail_tb1 ( k1 INT NOT NULL, k2 STRING NOT NULL)
            DUPLICATE KEY(k1,k2) DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
            """
        exception "String Type should not be used in key column[k2]"
    }
    // create table if not exists with string column, insert and select ok
    def tbName = "str_tb"
    sql "drop table if exists ${tbName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tbName} (k1 VARCHAR(10) NULL, v1 STRING NULL) 
        UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    sql """
        INSERT INTO ${tbName} VALUES
         ("", ""),
         (NULL, NULL),
         (1, repeat("test1111", 8192)),
         (2, repeat("test1111", 131072))
        """
    order_qt_select_str_tb "select k1, md5(v1), length(v1) from ${tbName}"
}

