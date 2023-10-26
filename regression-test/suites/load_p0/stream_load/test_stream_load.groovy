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
import java.util.Date
import java.text.SimpleDateFormat

suite("test_stream_load", "p0") {
    sql "show tables"

    def tableName = "test_stream_load_strict"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4) SUM NULL,
            `v2` tinyint(4) REPLACE NULL,
            `v3` tinyint(4) REPLACE_IF_NOT_NULL NULL,
            `v4` smallint(6) REPLACE_IF_NOT_NULL NULL,
            `v5` int(11) REPLACE_IF_NOT_NULL NULL,
            `v6` bigint(20) REPLACE_IF_NOT_NULL NULL,
            `v7` largeint(40) REPLACE_IF_NOT_NULL NULL,
            `v8` datetime REPLACE_IF_NOT_NULL NULL,
            `v9` date REPLACE_IF_NOT_NULL NULL,
            `v10` char(10) REPLACE_IF_NOT_NULL NULL,
            `v11` varchar(6) REPLACE_IF_NOT_NULL NULL,
            `v12` decimal(27, 9) REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`k1`)
        (PARTITION partition_a VALUES [("-9223372036854775808"), ("100000")),
        PARTITION partition_b VALUES [("100000"), ("1000000000")),
        PARTITION partition_c VALUES [("1000000000"), ("10000000000")),
        PARTITION partition_d VALUES [("10000000000"), (MAXVALUE)))
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    
    // test strict_mode success
    streamLoad {
        table "${tableName}"

        set 'column_separator', '\t'
        set 'columns', 'k1, k2, v2, v10, v11'
        set 'partitions', 'partition_a, partition_b, partition_c, partition_d'
        set 'strict_mode', 'true'

        file 'test_strict_mode.csv'
        time 10000 // limit inflight 10s
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2"

    // test strict_mode fail
    streamLoad {
        table "${tableName}"

        set 'column_separator', '\t'
        set 'columns', 'k1, k2, v2, v10, v11'
        set 'partitions', 'partition_a, partition_b, partition_c, partition_d'
        set 'strict_mode', 'true'

        file 'test_strict_mode_fail.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(1, json.NumberFilteredRows)
        }
    }

    sql "sync"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          `id` int(11) NULL,
          `value` varchar(64) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    streamLoad {
        table "${tableName}"

        set 'line_delimiter', 'weizuo'
        set 'column_separator', '|'
        set 'columns', 'id, value'

        file 'test_line_delimiter.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(3, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    sql "sync"
    rowCount = sql "select count(1) from ${tableName}"
    assertEquals(3, rowCount[0][0])

    // test load_nullable_to_not_nullable
    def tableName2 = "load_nullable_to_not_nullable"
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
    CREATE TABLE IF NOT EXISTS `${tableName2}` (
        k1 int(32) NOT NULL,
        k2 smallint NOT NULL,
        k3 int NOT NULL,
        k4 bigint NOT NULL,
        k5 decimal(9, 3) NOT NULL,
        k6 char(5) NOT NULL,
        k10 date NOT NULL,
        k11 datetime NOT NULL,
        k7 varchar(20) NOT NULL,
        k8 double max NOT NULL,
        k9 float sum NOT NULL )
    AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k7)
    PARTITION BY RANGE(k2) (
        PARTITION partition_a VALUES LESS THAN MAXVALUE
    )
    DISTRIBUTED BY HASH(k1, k2, k5)
    BUCKETS 3
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
    """

    streamLoad {
        table "${tableName2}"

        set 'column_separator', '\t'
        set 'columns', 'col,k1=year(col),k2=month(col),k3=month(col),k4=day(col),k5=7.7,k6="a",k10=date(col),k11=FROM_UNIXTIME(2019,"%Y-%m-%dT%H:%i:%s"),k7="k7",k8=month(col),k9=day(col)'

        file 'test_time.data'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_sql1 " SELECT * FROM ${tableName2}"

    // test common case
    def tableName3 = "test_all"
    def tableName4 = "test_less_col"
    def tableName5 = "test_bitmap_and_hll"
    def tableName6 = "test_unique_key"
    def tableName7 = "test_unique_key_with_delete"
    def tableName8 = "test_array"
    def tableName10 = "test_struct"
    sql """ DROP TABLE IF EXISTS ${tableName3} """
    sql """ DROP TABLE IF EXISTS ${tableName4} """
    sql """ DROP TABLE IF EXISTS ${tableName5} """
    sql """ DROP TABLE IF EXISTS ${tableName6} """
    sql """ DROP TABLE IF EXISTS ${tableName7} """
    sql """ DROP TABLE IF EXISTS ${tableName8} """
    sql """ DROP TABLE IF EXISTS ${tableName10} """
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName3} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `k3` smallint(6) NULL,
      `k4` bigint(20) NULL,
      `k5` largeint(40) NULL,
      `k6` float NULL,
      `k7` double NULL,
      `k8` decimal(9, 0) NULL,
      `k9` char(10) NULL,
      `k10` varchar(1024) NULL,
      `k11` text NULL,
      `k12` date NULL,
      `k13` datetime NULL
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName4} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `k3` smallint(6) NULL,
      `k4` bigint(20) NULL,
      `k5` largeint(40) NULL
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName5} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `v1` bitmap bitmap_union,
      `v2` hll hll_union
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName6} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `v1` varchar(1024)
    ) ENGINE=OLAP
    UNIQUE KEY(k1, k2)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName7} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `v1` varchar(1024)
    ) ENGINE=OLAP
    UNIQUE KEY(k1, k2)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "function_column.sequence_type" = "int",
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName8} (
      `k1` INT(11) NULL COMMENT "",
      `k2` ARRAY<SMALLINT> NULL COMMENT "",
      `k3` ARRAY<INT(11)> NULL COMMENT "",
      `k4` ARRAY<BIGINT> NULL COMMENT "",
      `k5` ARRAY<CHAR> NULL COMMENT "",
      `k6` ARRAY<VARCHAR(20)> NULL COMMENT "",
      `k7` ARRAY<DATE> NULL COMMENT "", 
      `k8` ARRAY<DATETIME> NULL COMMENT "",
      `k9` ARRAY<FLOAT> NULL COMMENT "",
      `k10` ARRAY<DOUBLE> NULL COMMENT "",
      `k11` ARRAY<DECIMAL(20, 6)> NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName10} (
      `k1` INT(11) NULL COMMENT "",
      `k2` STRUCT<
               f1:SMALLINT,
               f2:INT(11),
               f3:BIGINT,
               f4:CHAR,
               f5:VARCHAR(20),
               f6:DATE,
               f7:DATETIME,
               f8:FLOAT,
               f9:DOUBLE,
               f10:DECIMAL(20, 6)> NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    // load all columns
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','

        file 'all_types.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2500, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_all11 "SELECT count(*) FROM ${tableName3}" // 2500
    order_qt_all12 "SELECT count(*) FROM ${tableName3} where k1 <= 10"  // 11
    sql """truncate table ${tableName3}"""
    sql """sync"""

    // load part of columns
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','
        set 'columns', 'k1, k2'

        file 'all_types.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberLoadedRows)
        }
    }
    sql "sync"

    // load with skip 2 columns, with gzip
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','
        set 'columns', 'k1, k2, k3, k4, tmp1, tmp2, k7, k8, k9, k10, k11, k12, k13'
        set 'compress_type', 'gz'

        file 'all_types.csv.gz'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2500, json.NumberTotalRows)
            assertEquals(2500, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    order_qt_all21 "SELECT count(*) FROM ${tableName3}" // 2500
    order_qt_all22 "SELECT count(*) FROM ${tableName3} where k1 is null"  // 0
    order_qt_all23 "SELECT count(*) FROM ${tableName3} where k5 is null"  // 2500
    order_qt_all24 "SELECT count(*) FROM ${tableName3} where k6 is null"  // 2500
    sql """truncate table ${tableName3}"""
    sql """sync"""

    // load with column mapping and where predicate
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','
        set 'columns', 'k1, k2, k3, k4, tmp5, k6, tmpk7, k8, k9, k10, k11, k12, k13, k7=tmpk7+1'
        set 'where', 'k1 <= 10'

        file 'all_types.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2500, json.NumberTotalRows)
            assertEquals(11, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(2489, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    order_qt_all31 "SELECT count(*) FROM ${tableName3}" // 11
    order_qt_all32 "SELECT count(*) FROM ${tableName3} where k7 >= 7"  // 11
    order_qt_all33 "SELECT count(*) FROM ${tableName3} where k5 is null"  // 11
    sql """truncate table ${tableName3}"""
    sql """sync"""

    // load without strict_mode
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','
        set 'columns', 'tmpk1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k1=k13'

        file 'all_types.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2500, json.NumberTotalRows)
            assertEquals(2500, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    order_qt_all41 "SELECT count(*) FROM ${tableName3} where k1 is null" // 2500
    sql """truncate table ${tableName3}"""
    sql """sync"""

    // load with strict_mode false and max_filter_ratio
    streamLoad {
        table "${tableName4}"

        set 'column_separator', ','
        set 'columns', 'k1, k2, k3, k4, tmpk5, tmpk6, tmpk7, tmpk8, tmpk9, tmpk10, tmpk11, tmpk12, k5'
        set 'max_filter_ratio', '1'
        set 'strict_mode', 'true'

        file 'all_types.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2500, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
            assertEquals(2500, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    order_qt_all51 "SELECT count(*) FROM ${tableName4}" // 0
    sql """truncate table ${tableName4}"""
    sql """sync"""

    // load with strict_mode true and max_filter_ratio
    streamLoad {
        table "${tableName4}"

        set 'column_separator', ','
        set 'columns', 'k1, k2, k3, k4, tmpk5, tmpk6, tmpk7, tmpk8, tmpk9, tmpk10, tmpk11, tmpk12, k5'
        set 'max_filter_ratio', '0'
        set 'strict_mode', 'true'

        file 'all_types.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberLoadedRows)
        }
    }
    sql "sync"
    order_qt_all61 "SELECT count(*) FROM ${tableName4}" // 0
    sql """truncate table ${tableName4}"""
    sql """sync"""

    // load bitmap and hll with bzip2
    streamLoad {
        table "${tableName5}"

        set 'column_separator', ','
        set 'columns', 'k1, k2, tmp1, tmp2, v1=to_bitmap(tmp1), v2=hll_hash(tmp2)'
        set 'compress_type', 'bz2'

        file 'bitmap_hll.csv.bz2'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1025, json.NumberTotalRows)
            assertEquals(1025, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    order_qt_all71 "SELECT k1, k2, bitmap_union_count(v1), HLL_UNION_AGG(v2) FROM ${tableName5} group by k1, k2" // 1,2,1025,1028
    sql """truncate table ${tableName5}"""
    sql """sync"""

    // load unique key
    streamLoad {
        table "${tableName6}"

        set 'column_separator', ','
        set 'compress_type', 'lz4'

        file 'unique_key.csv.lz4'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(8001, json.NumberTotalRows)
            assertEquals(8001, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    order_qt_all81 "SELECT count(*) from ${tableName6}" // 2
    sql """truncate table ${tableName6}"""
    sql """sync"""

    // load unique key with delete and sequence
    streamLoad {
        table "${tableName7}"

        set 'column_separator', ','
        set 'columns', 'k1,k2,v1,del,seq'
        set 'delete', 'del=1'
        set 'merge_type', 'merge'
        set 'function_column.sequence_col', 'seq'

        file 'unique_key_with_delete.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(6, json.NumberTotalRows)
            assertEquals(6, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    order_qt_all91 "SELECT count(*) from ${tableName7}" // 2
    sql """truncate table ${tableName7}"""
    sql """sync"""

    // ===== test array stream load
    // malformat without strictmode
    streamLoad {
        table "${tableName8}"

        set 'column_separator', '|'

        file 'array_malformat.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(5, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    order_qt_all101 "SELECT * from ${tableName8}" // 5
    sql """truncate table ${tableName8}"""
    sql """sync"""

    // malformat with strictmode
    streamLoad {
        table "${tableName8}"

        set 'column_separator', '|'
        set 'strict_mode', 'true'

        file 'array_malformat.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(3, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"

    // normal load
    streamLoad {
        table "${tableName8}"

        set 'column_separator', '|'

        file 'array_normal.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(9, json.NumberTotalRows)
            assertEquals(9, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    order_qt_all102 "SELECT * from ${tableName8}" // 8
    sql """truncate table ${tableName8}"""
    sql """sync"""

    // malformat with mismatch array type
    streamLoad {
        table "${tableName8}"

        set 'column_separator', '|'
        set 'columns', 'k1,k2,k3,k4,k5,k6,k7,k8,k9,b10,k11,k10=array_remove(cast(k5 as array<bigint>), 1)'

        file 'array_normal.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains('Don\'t support load from type'))
        }
    }
    sql "sync"

    // ===== test struct stream load
    // malformat without strictmode
    streamLoad {
        table "${tableName10}"

        set 'column_separator', '|'

        file 'struct_malformat.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(5, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    qt_all111 "SELECT * from ${tableName10} order by k1" // 5
    sql """truncate table ${tableName10}"""
    sql """sync"""

    // malformat with strictmode
    streamLoad {
        table "${tableName10}"

        set 'column_separator', '|'
        set 'strict_mode', 'true'

        file 'struct_malformat.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(3, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"

    // normal load
    streamLoad {
        table "${tableName10}"

        set 'column_separator', '|'

        file 'struct_normal.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(13, json.NumberTotalRows)
            assertEquals(13, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    sql "sync"
    qt_all112 "SELECT * from ${tableName10} order by k1" // 10
    sql """truncate table ${tableName10}"""
    sql """sync"""

    // test immutable partition success
    def tableName9 = "test_immutable_partition"
    sql """ DROP TABLE IF EXISTS ${tableName9} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName9} (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4) SUM NULL,
            `v2` tinyint(4) REPLACE NULL,
            `v3` tinyint(4) REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`k1`)
        (PARTITION partition_a VALUES [("-9223372036854775808"), ("10")),
        PARTITION partition_b VALUES [("10"), ("20")),
        PARTITION partition_c VALUES [("20"), ("30")),
        PARTITION partition_d VALUES [("30"), ("40")))
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql """ALTER TABLE ${tableName9} ADD PARTITION partition_e VALUES less than ('3000') properties ('mutable' = 'false')"""
    sql """ALTER TABLE ${tableName9} MODIFY PARTITION partition_b set ('mutable' = 'false')"""

    streamLoad {
        table "${tableName9}"

        set 'column_separator', '\t'
        set 'columns', 'k1, k2, v1, v2, v3'
        set 'partitions', 'partition_a, partition_b, partition_c, partition_d, partition_e'
        set 'strict_mode', 'true'

        file 'test_immutable_partition.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(11, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(5, json.NumberUnselectedRows)
        }
    }
    
    sql "sync"
    order_qt_sql1 "select * from ${tableName9} order by k1, k2"

    // test common user
    def tableName13 = "test_common_user"
    sql """ DROP TABLE IF EXISTS ${tableName13} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName13} (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4) SUM NULL,
            `v2` tinyint(4) REPLACE NULL,
            `v3` tinyint(4) REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`k1`)
        (PARTITION partition_a VALUES [("-9223372036854775808"), ("10")),
        PARTITION partition_b VALUES [("10"), ("20")),
        PARTITION partition_c VALUES [("20"), ("30")),
        PARTITION partition_d VALUES [("30"), ("40")))
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    
    sql """create USER common_user@'%' IDENTIFIED BY '123456'"""
    sql """GRANT LOAD_PRIV ON *.* TO 'common_user'@'%';"""

    streamLoad {
        table "${tableName13}"

        set 'column_separator', '|'
        set 'columns', 'k1, k2, v1, v2, v3'
        set 'strict_mode', 'true'
        set 'Authorization', 'Basic  Y29tbW9uX3VzZXI6MTIzNDU2'

        file 'test_auth.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    
    sql "sync"
    sql """DROP USER 'common_user'@'%'"""

    // test default value
    def tableName14 = "test_default_value"
    sql """ DROP TABLE IF EXISTS ${tableName14} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName14} (
            `k1` bigint(20) NULL DEFAULT "1",
            `k2` bigint(20) NULL ,
            `v1` tinyint(4) NULL,
            `v2` tinyint(4) NULL,
            `v3` tinyint(4) NULL,
            `v4` DATETIME NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    streamLoad {
        table "${tableName14}"

        set 'column_separator', '|'
        set 'columns', 'k2, v1, v2, v3'
        set 'strict_mode', 'true'

        file 'test_default_value.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    
    sql "sync"
    def res = sql "select * from ${tableName14}"
    def time = res[0][5].toString().split("T")[0].split("-")
    def year = time[0].toString()
    SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd")
    def now = sdf.format(new Date()).toString().split("-")

    // parse time is correct
    // Due to the time difference in parsing, should deal with three situations:
    // 2023-6-29 -> 2023-6-30
    // 2023-6-30 -> 2023-7-1
    // 2023-12-31 -> 2024-1-1
    // now only compare year simply, you can retry if this test is error.
    assertEquals(year, now[0])
    // parse k1 default value
    assertEquals(res[0][0], 1)
    assertEquals(res[1][0], 1)

}

