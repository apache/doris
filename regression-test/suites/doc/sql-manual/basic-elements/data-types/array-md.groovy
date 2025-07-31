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

suite("array-md", "p0, nonConcurrent") {
    
    def tableName = "array_table"
    sql """
        drop table if exists ${tableName};
    """
    sql """ set enable_decimal256 = true; """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_boolean ARRAY<BOOLEAN>,
            array_tinyint ARRAY<TINYINT>,
            array_smallint ARRAY<SMALLINT>,
            array_int ARRAY<INT>,
            array_bigint ARRAY<BIGINT>,
            array_largeint ARRAY<LARGEINT>,
            array_float ARRAY<FLOAT>,
            array_double ARRAY<DOUBLE>,
            array_decimal32 ARRAY<DECIMAL(7, 2)>,
            array_decimal64 ARRAY<DECIMAL(15, 3)>,
            array_decimal128 ARRAY<DECIMAL(25, 5)>,
            array_decimal256 ARRAY<DECIMAL(40, 7)>,
            array_string ARRAY<STRING>,
            array_varchar ARRAY<VARCHAR(10)>,
            array_char ARRAY<CHAR(10)>,
            array_date ARRAY<DATE>,
            array_datetime ARRAY<DATETIME>,
            array_ipv4 ARRAY<IPV4>,
            array_ipv6 ARRAY<IPV6>,
            array_struct ARRAY<STRUCT<id: INT, name: STRING>>,
            array_array ARRAY<ARRAY<INT>>,
            array_map ARRAY<MAP<STRING, INT>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO ${tableName} (id, array_boolean, array_tinyint, array_smallint, array_int, array_bigint, array_largeint, array_float, array_double, array_decimal32, array_decimal64, array_decimal128, array_decimal256, array_string, array_varchar, array_char, array_date, array_datetime, array_ipv4, array_ipv6, array_struct, array_array, array_map) VALUES
        (1, [true, false, true], [1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3], ['hello', 'world'], ['hello', 'world'], ['hello', 'world'], ['2021-01-01', '2021-01-02', '2021-01-03'], ['2021-01-01 00:00:00', '2021-01-02 00:00:00', '2021-01-03 00:00:00'], ['192.168.1.1', '192.168.1.2', '192.168.1.3'], ['::1', '::2', '::3'], ARRAY(STRUCT(1, 'John'), STRUCT(2, 'Jane')), [[1, 2, 3], [4, 5, 6]], ARRAY(MAP('key1', 1), MAP('key2', 2)));
    """

    qt_sql """
        SELECT * FROM ${tableName};
    """

    sql """DROP TABLE IF EXISTS ${tableName};"""
    test {
        sql """
            CREATE TABLE IF NOT EXISTS  ${tableName} (
                id INT,
                array_nested ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<INT>>>>>>>>>>>>
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Type exceeds the maximum nesting depth of 9"
    }
    
    test {
        sql """ SELECT CAST(ARRAY(1, 2, 3) AS STRING) """
        exception "can not cast from origin type ARRAY<TINYINT> to target type=TEXT"
    }

    qt_sql """ SELECT CAST("[1,2,3]" AS ARRAY<INT>) """

    qt_sql """ SELECT CAST([1,2,3] AS ARRAY<STRING>) """

    sql """ DROP TABLE IF EXISTS varaint_table; """
    sql """
        CREATE TABLE IF NOT EXISTS varaint_table (
            id INT,
            var VARIANT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO varaint_table (id, var) VALUES (1, '[1,2,3]') """

    qt_sql """ SELECT cast(var as ARRAY<INT>) FROM varaint_table; """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_int ARRAY<INT> REPLACE_IF_NOT_NULL,
            array_string ARRAY<STRING> REPLACE,
        ) ENGINE=OLAP
        AGGREGATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                array_int ARRAY<INT> SUM
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type SUM is not compatible with primitive type ARRAY<INT>"
    }

    test{
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                array_int ARRAY<INT> MIN
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type MIN is not compatible with primitive type ARRAY<INT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                array_int ARRAY<INT> MAX
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type MAX is not compatible with primitive type ARRAY<INT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                array_int ARRAY<INT> HLL_UNION
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type HLL_UNION is not compatible with primitive type ARRAY<INT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                array_int ARRAY<INT> BITMAP_UNION
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type BITMAP_UNION is not compatible with primitive type ARRAY<INT>"
    }
     
    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                array_int ARRAY<INT> QUANTILE_UNION
            ) ENGINE=OLAP   
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type QUANTILE_UNION is not compatible with primitive type ARRAY<INT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                array_int ARRAY<INT>,
                array_value ARRAY<STRING>
            ) ENGINE=OLAP
            DUPLICATE KEY(array_int)
            DISTRIBUTED BY HASH(array_int) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Array can only be used in the non-key column of the duplicate table at present"
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_int ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_int) VALUES (1, [1, 2, 3]), (2, [1, 2, 3]) """

    qt_sql """ select array_int, count(*) from ${tableName} group by array_int order by array_int """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_int ARRAY<ARRAY<INT>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_int) VALUES (1, [[1, 2, 3], [1, 2, 3]]), (2, [[1, 2, 3], [2, 2, 3]]) """

    qt_sql """ select array_int, count(*) from ${tableName} group by array_int order by array_int """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_struct ARRAY<STRUCT<id: INT, name: STRING>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_struct) VALUES (1, ARRAY(STRUCT(1, 'John'), STRUCT(2, 'Jane'))), (2, ARRAY(STRUCT(1, 'John'), STRUCT(2, 'Jane'))) """

    qt_sql """ select array_struct, count(*) from ${tableName} group by array_struct order by array_struct """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_map ARRAY<MAP<STRING, INT>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_map) VALUES (1, ARRAY(MAP('key1', 1), MAP('key2', 2))), (2, ARRAY(MAP('key1', 1), MAP('key2', 2))) """

    qt_sql """ select array_map, count(*) from ${tableName} group by array_map order by array_map """

    sql """ DROP TABLE IF EXISTS array_table_1; """
    sql """
        CREATE TABLE IF NOT EXISTS array_table_1 (
            id INT,
            array_int ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO array_table_1 (id, array_int) VALUES (1, [1, 2, 3]), (2, [1, 2, 3]) """

     sql """ DROP TABLE IF EXISTS array_table_2; """
    sql """
        CREATE TABLE IF NOT EXISTS array_table_2 (
            id INT,
            array_int ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO array_table_2 (id, array_int) VALUES (1, [1, 2, 3]), (2, [1, 2, 3]) """
    qt_sql """ select * from array_table_1 join array_table_2 on array_contains(array_table_1.array_int,array_table_2.id) order by array_table_1.id, array_table_2.id """

    try {
        sql """ select * from array_table_1 join array_table_2 on array_table_1.array_int = array_table_2.array_int """
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_int ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_int) VALUES (1, [1, 2, 3]), (2, [1, 2, 4]), (3, [1, 2, 5]) """

    qt_sql """ select array_int, count(*) from ${tableName} group by array_int order by array_int """


    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_string ARRAY<STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_string) VALUES (1, ['hello', 'world']), (2, ['hello', 'world']), (3, ['hello', 'world']) """

    qt_sql """ select array_string, count(*) from ${tableName} group by array_string order by array_string """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_float ARRAY<FLOAT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_float) VALUES (1, [1.1, 2.2, 3.3]), (2, [1.1, 2.2, 3.3]) """

    qt_sql """ select array_float, count(*) from ${tableName} group by array_float order by array_float """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_double ARRAY<DOUBLE>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_double) VALUES (1, [1.1, 2.2, 3.3]), (2, [1.1, 2.2, 3.3]) """

    qt_sql """ select array_double, count(*) from ${tableName} group by array_double order by array_double """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_decimal32 ARRAY<DECIMAL(7, 2)>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_decimal32) VALUES (1, [1.1, 2.2, 3.3]), (2, [1.1, 2.2, 3.3]) """

    qt_sql """ select array_decimal32, count(*) from ${tableName} group by array_decimal32 order by array_decimal32 """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_decimal64 ARRAY<DECIMAL(15, 3)>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_decimal64) VALUES (1, [1.1, 2.2, 3.3]), (2, [1.1, 2.2, 3.3]) """

    qt_sql """ select array_decimal64, count(*) from ${tableName} group by array_decimal64 order by array_decimal64 """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_decimal128 ARRAY<DECIMAL(25, 5)>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_decimal128) VALUES (1, [1.1, 2.2, 3.3]), (2, [1.1, 2.2, 3.3]) """

    qt_sql """ select array_decimal128, count(*) from ${tableName} group by array_decimal128 order by array_decimal128 """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_decimal256 ARRAY<DECIMAL(40, 8)>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_decimal256) VALUES (1, [1.1, 2.2, 3.3]), (2, [1.1, 2.2, 3.3]) """

    qt_sql """ select array_decimal256, count(*) from ${tableName} group by array_decimal256 order by array_decimal256 """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_date ARRAY<DATE>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_date) VALUES (1, ['2021-01-01', '2021-01-02', '2021-01-03']), (2, ['2021-01-01', '2021-01-02', '2021-01-03']) """

    qt_sql """ select array_date, count(*) from ${tableName} group by array_date order by array_date """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_datetime ARRAY<DATETIME>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_datetime) VALUES (1, ['2021-01-01 00:00:00', '2021-01-02 00:00:00', '2021-01-03 00:00:00']), (2, ['2021-01-01 00:00:00', '2021-01-02 00:00:00', '2021-01-03 00:00:00']) """

    qt_sql """ select array_datetime, count(*) from ${tableName} group by array_datetime order by array_datetime """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_ipv4 ARRAY<IPV4>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_ipv4) VALUES (1, ['192.168.1.1', '192.168.1.2', '192.168.1.3']), (2, ['192.168.1.1', '192.168.1.2', '192.168.1.3']) """

    qt_sql """ select array_ipv4, count(*) from ${tableName} group by array_ipv4 order by array_ipv4 """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_ipv6 ARRAY<IPV6>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_ipv6) VALUES (1, ['2001:0db8:85a3:0000:0000:8a2e:0370:7334', '2001:0db8:85a3:0000:0000:8a2e:0370:7334']), (2, ['2001:0db8:85a3:0000:0000:8a2e:0370:7334', '2001:0db8:85a3:0000:0000:8a2e:0370:7334']) """

    qt_sql """ select array_ipv6, count(*) from ${tableName} group by array_ipv6 order by array_ipv6 """
   
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            str STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${tableName} (id, str) VALUES (1, 'hello'), (2, 'world'), (3, 'hello') """

    qt_sql """ SELECT ARRAY(str, id) FROM ${tableName} """

    test {
        sql """ SELECT [str, id] FROM ${tableName} """
        exception "errCode = 2"
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_varchar ARRAY<VARCHAR(10)>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, array_varchar) VALUES (1, ['hello', 'world']), (2, ['hello', 'world']), (3, ['hello', 'world']) """
    test {
         sql """ ALTER TABLE ${tableName} MODIFY COLUMN array_varchar ARRAY<VARCHAR(10)> DEFAULT NULL  """
         exception "Nothing is changed. please check your alter stmt"
    }

    test {
        sql """ ALTER TABLE ${tableName} MODIFY COLUMN array_varchar ARRAY<VARCHAR(9)> """
        exception "Shorten type length is prohibited, srcType=varchar(10), dstType=varchar(9)"
    }

    sql """ ALTER TABLE ${tableName} MODIFY COLUMN array_varchar ARRAY<VARCHAR(20)> """
   

    qt_sql """ select array_varchar[0], array_varchar[1], array_varchar[2], array_varchar[3] from ${tableName} """
    qt_sql """ select element_at(array_varchar, 0), element_at(array_varchar, 1), element_at(array_varchar, 2), element_at(array_varchar, 3) from ${tableName} """

    sql """ DROP TABLE IF EXISTS array_table; """
    sql """
        CREATE TABLE `array_table` (
            `k` int NOT NULL,
            `array_column` array<STRING>,
            INDEX idx_array_column (array_column) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql """ insert into array_table values (1, ['ab', 'cd', 'ef']), (2, ['gh', 'ij', 'kl']), (3, ['mn', 'op', 'qr']); """

     def queryAndCheck = { String sqlQuery, int expectedFilteredRows = -1 ->
      def checkpoints_name = "segment_iterator.inverted_index.filtered_rows"
      try {
          GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
          GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [filtered_rows: expectedFilteredRows])
          sql "set experimental_enable_parallel_scan = false"
          sql " set inverted_index_skip_threshold = 0 "
          sql " set enable_common_expr_pushdown_for_inverted_index = true"
          sql " set enable_common_expr_pushdown = true"
          sql " set enable_parallel_scan = false"
          sql "sync"
          sql "${sqlQuery}"
      } finally {
          GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
          GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
      }
    }
    queryAndCheck ("""select count() from array_table where ARRAY_CONTAINS(array_column, 'ef')""", 2)

    queryAndCheck ("""select count() from array_table where ARRAYS_OVERLAP(array_column, ['ab', 'op'])""", 1)

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_int ARRAY<INT>,
            INDEX idx_array_int (array_int) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ insert into ${tableName} values (1, [1, 2, 3]), (2, [4, 5, 6]), (3, [7, 8, 9]); """

    test {
        sql """ delete from ${tableName} where array_contains(array_int, 5); """
        exception "Can not apply delete condition to column type: array<int>"
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            two_dim_array ARRAY<ARRAY<INT>>,
            three_dim_array ARRAY<ARRAY<ARRAY<STRING>>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES (1, [[1, 2, 3], [4, 5, 6]], [[['ab', 'cd', 'ef'], ['gh', 'ij', 'kl']], [['mn', 'op', 'qr'], ['st', 'uv', 'wx']]]) """
    sql """ INSERT INTO ${tableName} VALUES (2, ARRAY(ARRAY(1, 2, 3), ARRAY(4, 5, 6)), ARRAY(ARRAY(ARRAY('ab', 'cd', 'ef'), ARRAY('gh', 'ij', 'kl')), ARRAY(ARRAY('mn', 'op', 'qr'), ARRAY('st', 'uv', 'wx')))) """

    qt_sql """ SELECT two_dim_array[1][2], three_dim_array[1][1][2] FROM ${tableName} ORDER BY id """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_map ARRAY<MAP<STRING, INT>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql """ INSERT INTO ${tableName} VALUES (1, ARRAY(MAP('key1', 1), MAP('key2', 2))) """
    sql """ INSERT INTO ${tableName} VALUES (2, ARRAY(MAP('key1', 1), MAP('key2', 2))) """

    qt_sql """ SELECT array_map[1], array_map[2] FROM ${tableName} ORDER BY id """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_struct ARRAY<STRUCT<id: INT, name: STRING>>,
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql """ INSERT INTO ${tableName} VALUES (1, ARRAY(STRUCT(1, 'John'), STRUCT(2, 'Jane'))) """
    sql """ INSERT INTO ${tableName} VALUES (2, ARRAY(STRUCT(1, 'John'), STRUCT(2, 'Jane'))) """

    qt_sql """ SELECT array_struct[1], array_struct[2] FROM ${tableName} ORDER BY id """
    
}