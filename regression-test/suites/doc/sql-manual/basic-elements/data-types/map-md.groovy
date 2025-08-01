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

suite("map-md", "p0") {
    
    def tableName = "map_table"
    sql """
        drop table if exists ${tableName};
    """
    sql """ set enable_decimal256 = true; """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            map_string_int MAP<STRING, INT>,
            map_string_string MAP<STRING, STRING>,
            map_string_boolean MAP<STRING, BOOLEAN>,
            map_string_tinyint MAP<STRING, TINYINT>,
            map_string_smallint MAP<STRING, SMALLINT>,
            map_string_bigint MAP<STRING, BIGINT>,
            map_string_largeint MAP<STRING, LARGEINT>,
            map_string_float MAP<STRING, FLOAT>,
            map_string_double MAP<STRING, DOUBLE>,
            map_string_decimal32 MAP<STRING, DECIMAL(7, 2)>,
            map_string_decimal64 MAP<STRING, DECIMAL(15, 3)>,
            map_string_decimal128 MAP<STRING, DECIMAL(25, 5)>,
            map_string_decimal256 MAP<STRING, DECIMAL(40, 8)>,
            map_string_date MAP<STRING, DATE>,
            map_string_datetime MAP<STRING, DATETIME>,
            map_string_ipv4 MAP<STRING, IPV4>,
            map_string_ipv6 MAP<STRING, IPV6>,
            map_string_array MAP<STRING, ARRAY<INT>>,
            map_string_struct MAP<STRING, STRUCT<id: INT, name: STRING>>,
            map_string_map MAP<STRING, MAP<STRING, INT>>,
            map_string_char MAP<STRING, CHAR(10)>,
            map_string_varchar MAP<STRING, VARCHAR(10)>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO ${tableName}  VALUES
        (1, MAP('key1', 1, 'key2', 2),  MAP('key1', 'value1', 'key2', 'value2'), MAP('key1', true, 'key2', false),
        MAP('key1', 1, 'key2', 2), MAP('key1', 1, 'key2', 2), MAP('key1', 1, 'key2', 2), MAP('key1', 1, 'key2', 2), MAP('key1', 1.1, 'key2', 2.2), MAP('key1', 1.1, 'key2', 2.2),
        MAP('key1', 1.1, 'key2', 2.2), MAP('key1', 1.1, 'key2', 2.2), MAP('key1', 1.1, 'key2', 2.2), MAP('key1', 1.1, 'key2', 2.2), MAP('key1', '2021-01-01', 'key2', '2021-01-02'),
        MAP('key1', '2021-01-01 00:00:00', 'key2', '2021-01-02 00:00:00'), MAP('key1', '192.168.1.1', 'key2', '192.168.1.2'), MAP('key1', '::1', 'key2', '::2'),
        MAP('key1', [1, 2, 3], 'key2', [4, 5, 6]), MAP('key1', STRUCT(1, 'John'), 'key2', STRUCT(2, 'Jane')),
        MAP('key1', MAP('nested_key1', 1), 'key2', MAP('nested_key2', 2)), MAP('key1', '1234567890', 'key2', '1234567890'), MAP('key1', '1234567890', 'key2', '1234567890'));
    """

    qt_sql """
        SELECT * FROM ${tableName};
    """

    qt_sql """ select count() from ${tableName} group by map_string_int order by map_string_int """
    qt_sql """ select count() from ${tableName} group by map_string_string order by map_string_string """
    qt_sql """ select count() from ${tableName} group by map_string_boolean order by map_string_boolean """
    qt_sql """ select count() from ${tableName} group by map_string_tinyint order by map_string_tinyint """
    qt_sql """ select count() from ${tableName} group by map_string_smallint order by map_string_smallint """
    qt_sql """ select count() from ${tableName} group by map_string_bigint order by map_string_bigint """
    qt_sql """ select count() from ${tableName} group by map_string_largeint order by map_string_largeint """
    qt_sql """ select count() from ${tableName} group by map_string_float order by map_string_float """
    qt_sql """ select count() from ${tableName} group by map_string_double order by map_string_double """
    qt_sql """ select count() from ${tableName} group by map_string_decimal32 order by map_string_decimal32 """
    qt_sql """ select count() from ${tableName} group by map_string_decimal64 order by map_string_decimal64 """
    qt_sql """ select count() from ${tableName} group by map_string_decimal128 order by map_string_decimal128 """
    qt_sql """ select count() from ${tableName} group by map_string_decimal256 order by map_string_decimal256 """
    qt_sql """ select count() from ${tableName} group by map_string_date order by map_string_date """
    qt_sql """ select count() from ${tableName} group by map_string_datetime order by map_string_datetime """
    qt_sql """ select count() from ${tableName} group by map_string_ipv4 order by map_string_ipv4 """
    qt_sql """ select count() from ${tableName} group by map_string_ipv6 order by map_string_ipv6 """
    qt_sql """ select count() from ${tableName} group by map_string_array order by map_string_array """
    qt_sql """ select count() from ${tableName} group by map_string_struct order by map_string_struct """
    qt_sql """ select count() from ${tableName} group by map_string_map order by map_string_map """
    qt_sql """ select count() from ${tableName} group by map_string_char order by map_string_char """
    qt_sql """ select count() from ${tableName} group by map_string_varchar order by map_string_varchar """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            map_bool_string MAP<BOOLEAN, STRING>,
            map_tinyint_string MAP<TINYINT, STRING>,
            map_smallint_string MAP<SMALLINT, STRING>,
            map_bigint_string MAP<BIGINT, STRING>,
            map_largeint_string MAP<LARGEINT, STRING>,
            map_float_string MAP<FLOAT, STRING>,
            map_double_string MAP<DOUBLE, STRING>,
            map_decimal32_string MAP<DECIMAL(7, 2), STRING>,
            map_decimal64_string MAP<DECIMAL(15, 3), STRING>,
            map_decimal128_string MAP<DECIMAL(25, 5), STRING>,
            map_decimal256_string MAP<DECIMAL(40, 8), STRING>,
            map_date_string MAP<DATE, STRING>,
            map_datetime_string MAP<DATETIME, STRING>,
            map_ipv4_string MAP<IPV4, STRING>,
            map_ipv6_string MAP<IPV6, STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, MAP(true, 'value1', false, 'value2'), MAP(1, 'value1', 2, 'value2'), MAP(1, 'value1', 2, 'value2'), MAP(1, 'value1', 2, 'value2'),
        MAP(1, 'value1', 2, 'value2'), MAP(1.1, 'value1', 2.2, 'value2'), MAP(1.1, 'value1', 2.2, 'value2'), MAP(1.1, 'value1', 2.2, 'value2'),
        MAP(1.1, 'value1', 2.2, 'value2'), MAP(1.1, 'value1', 2.2, 'value2'), MAP(1.1, 'value1', 2.2, 'value2'), MAP('2021-01-01', 'value1', '2021-01-02', 'value2'),
        MAP('2021-01-01 00:00:00', 'value1', '2021-01-02 00:00:00', 'value2'), MAP('192.168.1.1', 'value1', '192.168.1.2', 'value2'),
        MAP('::1', 'value1', '::2', 'value2'));
    """

    qt_sql """
        SELECT * FROM ${tableName};
    """

    sql """DROP TABLE IF EXISTS ${tableName};"""
    
    test {
        sql """
            CREATE TABLE IF NOT EXISTS  ${tableName} (
                id INT,
                map_nested MAP<STRING, MAP<STRING, MAP<STRING, MAP<STRING, MAP<STRING, MAP<STRING, MAP<STRING, MAP<STRING, MAP<STRING, MAP<STRING, MAP<STRING, MAP<STRING, INT>>>>>>>>>>>>
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
        sql """ SELECT CAST(MAP('key1', 1, 'key2', 2) AS STRING) """
        exception "can not cast from origin type MAP<VARCHAR(4),TINYINT> to target type=TEXT"
    }

    qt_sql """ SELECT CAST(MAP('key1', 1, 'key2', 2) AS MAP<STRING, STRING>) """

    qt_sql """ SELECT CAST('{"key1":1,"key2":2}' AS MAP<STRING, INT>) """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            map_string_int MAP<STRING, INT> REPLACE_IF_NOT_NULL,
            map_string_string MAP<STRING, STRING> REPLACE,
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
                map_string_int MAP<STRING, INT> SUM
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type SUM is not compatible with primitive type MAP<TEXT,INT>"
    }

    test{
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                map_string_int MAP<STRING, INT> MIN
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type MIN is not compatible with primitive type MAP<TEXT,INT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                map_string_int MAP<STRING, INT> MAX
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type MAX is not compatible with primitive type MAP<TEXT,INT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                map_string_int MAP<STRING, INT> HLL_UNION
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type HLL_UNION is not compatible with primitive type MAP<TEXT,INT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                map_string_int MAP<STRING, INT> BITMAP_UNION
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type BITMAP_UNION is not compatible with primitive type MAP<TEXT,INT>"
    }
     
    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                map_string_int MAP<STRING, INT> QUANTILE_UNION
            ) ENGINE=OLAP   
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type QUANTILE_UNION is not compatible with primitive type MAP<TEXT,INT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                map_string_int MAP<STRING, INT>,
                map_value MAP<STRING, STRING>
            ) ENGINE=OLAP
            DUPLICATE KEY(map_string_int)
            DISTRIBUTED BY HASH(map_string_int) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Map can only be used in the non-key column of the duplicate table at present"
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            map_string_int MAP<STRING, INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, map_string_int) VALUES (1, MAP('key1', 1, 'key2', 2)), (2, MAP('key1', 1, 'key2', 2)) """

    qt_sql """ select map_string_int, count(*) from ${tableName} group by map_string_int order by map_string_int """


    sql """ DROP TABLE IF EXISTS map_table_1; """
    sql """
        CREATE TABLE IF NOT EXISTS map_table_1 (
            id INT,
            map_string_int MAP<STRING, INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO map_table_1 (id, map_string_int) VALUES (1, MAP('key1', 1, 'key2', 2)), (2, MAP('key1', 1, 'key2', 2)) """

     sql """ DROP TABLE IF EXISTS map_table_2; """
    sql """
        CREATE TABLE IF NOT EXISTS map_table_2 (
            id INT,
            map_string_int MAP<STRING, INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO map_table_2 (id, map_string_int) VALUES (1, MAP('key1', 1, 'key2', 2)), (2, MAP('key1', 1, 'key2', 2)) """

    test {
        sql """ select * from map_table_1 join map_table_2 on map_table_1.map_string_int = map_table_2.map_string_int """
        exception "comparison predicate could not contains complex type: (map_string_int = map_string_int)"
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            map_string_int MAP<STRING, INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, map_string_int) VALUES (1, MAP('key1', 1, 'key2', 2)), (2, MAP('key1', 1, 'key2', 3)), (3, MAP('key1', 1, 'key2', 4)) """

    test {
        sql """ DELETE FROM ${tableName} WHERE map_contains_key(map_string_int, 'key1') """
        exception "Can not apply delete condition to column type: map<text,int>"
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            map_string_string MAP<STRING, STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, map_string_string) VALUES (1, MAP('key1', 'value1', 'key2', 'value2')), (2, MAP('key1', 'value1', 'key2', 'value2')), (3, MAP('key1', 'value1', 'key2', 'value2')) """

    qt_sql """ select map_string_string['key1'], map_string_string['key2'], map_string_string['key3'] from ${tableName} order by id """

    qt_sql """ SELECT element_at(map_string_string, 'key1') FROM ${tableName} order by id """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            map_string_int MAP<STRING, INT>,
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ insert into ${tableName} values (1, MAP('key1', 1, 'key2', 2)), (2, MAP('key3', 3, 'key4', 4)), (3, MAP('key5', 5, 'key6', 6)); """

    test {
        sql """ delete from ${tableName} where map_contains_key(map_string_int, 'key1'); """
        exception "Can not apply delete condition to column type: map<text,int>"
    }

    qt_sql """ SELECT MAP('Alice', 21, 'Bob', 23);"""

    qt_sql """ SELECT {'Alice': 20};"""

    qt_sql """ SELECT {'Alice': 20}['Alice']; """

    qt_sql """ SELECT ELEMENT_AT({'Alice': 20}, 'Alice');"""
    
    
    qt_sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            map_nested MAP<STRING, MAP<STRING, INT>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES (1, MAP('key1', MAP('key2', 1, 'key3', 2)))"""
    sql """ INSERT INTO ${tableName} VALUES (2, MAP('key1', MAP('key2', 1, 'key3', 2))) """

    qt_sql """ SELECT map_nested['key1']['key2'] FROM ${tableName} order by id """


    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            map_array MAP<STRING, ARRAY<INT>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES (1, MAP('key1', [1, 2, 3])), (2, MAP('key1', [4, 5, 6])) """

    qt_sql """ SELECT map_array['key1'][1] FROM ${tableName} order by id """
    
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            map_struct MAP<STRING, STRUCT<id: INT, name: STRING>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} VALUES (1, MAP('key1', STRUCT(1, 'John'), 'key2', STRUCT(3, 'Jane'))) """

    qt_sql """ SELECT STRUCT_ELEMENT(map_struct['key1'], 1), STRUCT_ELEMENT(map_struct['key1'], 'name') FROM ${tableName} order by id """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
      `k` INT NOT NULL,
      `map_varchar_int` MAP<VARCHAR(10), INT>,
      `map_int_varchar` MAP<INT, VARCHAR(10)>,
      `map_varchar_varchar` MAP<VARCHAR(10), VARCHAR(10)>
    ) ENGINE=OLAP
    DUPLICATE KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );
    """

    sql """ ALTER TABLE ${tableName} MODIFY COLUMN map_varchar_int MAP<VARCHAR(20), INT>; """
    sql """ ALTER TABLE ${tableName} MODIFY COLUMN map_int_varchar MAP<INT, VARCHAR(20)>; """
    sql """ ALTER TABLE ${tableName} MODIFY COLUMN map_varchar_varchar MAP<VARCHAR(20), VARCHAR(20)>; """

    qt_sql """ DESC ${tableName} """

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `k` INT NOT NULL,
                `map_varchar_int` MAP<VARCHAR(10), INT>,
                INDEX map_varchar_int_index (map_varchar_int) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "MAP<VARCHAR(10),INT> is not supported in INVERTED index. invalid index: map_varchar_int_index"
    }
}
