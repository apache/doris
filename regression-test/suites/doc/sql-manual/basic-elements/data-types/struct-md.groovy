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

suite("struct-md", "p0") {
    
    def tableName = "struct_table"
    sql """
        drop table if exists ${tableName};
    """
    sql """ set enable_decimal256 = true; """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            struct_basic STRUCT<id: INT, name: STRING>,
            struct_string_boolean STRUCT<name: STRING, active: BOOLEAN>,
            struct_multi_types STRUCT<tinyint_col: TINYINT, smallint_col: SMALLINT, bigint_col: BIGINT, largeint_col: LARGEINT>,
            struct_float_double STRUCT<float_col: FLOAT, double_col: DOUBLE>,
            struct_decimals STRUCT<decimal32_col: DECIMAL(7, 2), decimal64_col: DECIMAL(15, 3), decimal128_col: DECIMAL(25, 5), decimal256_col: DECIMAL(40, 8)>,
            struct_datetime STRUCT<date_col: DATE, datetime_col: DATETIME>,
            struct_network STRUCT<ipv4_col: IPV4, ipv6_col: IPV6>,
            struct_array STRUCT<id: INT, tags: ARRAY<STRING>>,
            struct_map STRUCT<id: INT, attributes: MAP<STRING, INT>>,
            struct_nested STRUCT<person: STRUCT<name: STRING, age: INT>, address: STRUCT<city: STRING, zip: INT>>,
            struct_char_varchar STRUCT<char_col: CHAR(10), varchar_col: VARCHAR(10) COMMENT 'test_coment'>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, STRUCT(1, 'John'), STRUCT('Alice', true), STRUCT(1, 2, 3, 4), STRUCT(1.1, 2.2), 
         STRUCT(1.1, 2.2, 3.3, 4.4), STRUCT('2021-01-01', '2021-01-01 00:00:00'), 
         STRUCT('192.168.1.1', '::1'), STRUCT(1, ['tag1', 'tag2']), STRUCT(1, MAP('key1', 1, 'key2', 2)),
         STRUCT(STRUCT('John', 25), STRUCT('New York', 10001)), STRUCT('1234567890', '1234567890'));
    """

    qt_sql """
        SELECT * FROM ${tableName};
    """

    qt_sql """ select struct_basic, count(*) from ${tableName} group by struct_basic order by struct_basic """
    qt_sql """ select struct_string_boolean, count(*) from ${tableName} group by struct_string_boolean order by struct_string_boolean """
    qt_sql """ select struct_multi_types, count(*) from ${tableName} group by struct_multi_types order by struct_multi_types """
    qt_sql """ select struct_float_double, count(*) from ${tableName} group by struct_float_double order by struct_float_double """
    qt_sql """ select struct_decimals, count(*) from ${tableName} group by struct_decimals order by struct_decimals """
    qt_sql """ select struct_datetime, count(*) from ${tableName} group by struct_datetime order by struct_datetime """
    qt_sql """ select struct_network, count(*) from ${tableName} group by struct_network order by struct_network """
    qt_sql """ select struct_array, count(*) from ${tableName} group by struct_array order by struct_array """
    qt_sql """ select struct_map, count(*) from ${tableName} group by struct_map order by struct_map """
    qt_sql """ select struct_nested, count(*) from ${tableName} group by struct_nested order by struct_nested """
    qt_sql """ select struct_char_varchar, count(*) from ${tableName} group by struct_char_varchar order by struct_char_varchar """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            struct_all_types STRUCT<
                bool_col: BOOLEAN,
                tinyint_col: TINYINT,
                smallint_col: SMALLINT,
                int_col: INT,
                bigint_col: BIGINT,
                largeint_col: LARGEINT,
                float_col: FLOAT,
                double_col: DOUBLE,
                decimal32_col: DECIMAL(7, 2),
                decimal64_col: DECIMAL(15, 3),
                decimal128_col: DECIMAL(25, 5),
                decimal256_col: DECIMAL(40, 8),
                string_col: STRING,
                date_col: DATE,
                datetime_col: DATETIME,
                ipv4_col: IPV4,
                ipv6_col: IPV6
            >
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, STRUCT(true, 1, 2, 3, 4, 5, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 'test', '2021-01-01', '2021-01-01 00:00:00', '192.168.1.1', '::1'));
    """

    qt_sql """
        SELECT * FROM ${tableName};
    """

    sql """DROP TABLE IF EXISTS ${tableName};"""
    
    test {
        sql """
            CREATE TABLE IF NOT EXISTS  ${tableName} (
                id INT,
                struct_nested STRUCT<
                    level1: STRUCT<
                        level2: STRUCT<
                            level3: STRUCT<
                                level4: STRUCT<
                                    level5: STRUCT<
                                        level6: STRUCT<
                                            level7: STRUCT<
                                                level8: STRUCT<
                                                    level9: STRUCT<
                                                        level10: STRUCT<
                                                            level11: INT
                                                        >
                                                    >
                                                >
                                            >
                                        >
                                    >
                                >
                            >
                        >
                    >
                >
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
        sql """ SELECT CAST(STRUCT(1, 'John') AS STRING) """
        exception "can not cast from origin type STRUCT<StructField ( name=col1, dataType=TINYINT, nullable=true ),StructField ( name=col2, dataType=VARCHAR(4), nullable=true )> to target type=TEXT"
    }

    qt_sql """ SELECT CAST(STRUCT(1, 'John') AS STRUCT<id: INT, name: STRING>) """

    qt_sql """ SELECT CAST('{"id":1,"name":"John"}' AS STRUCT<id: INT, name: STRING>) """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            struct_basic STRUCT<id: INT, name: STRING> REPLACE_IF_NOT_NULL,
            struct_other STRUCT<active: BOOLEAN, score: DOUBLE> REPLACE,
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
                struct_basic STRUCT<id: INT, name: STRING> SUM
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type SUM is not compatible with primitive type STRUCT<id:INT,name:TEXT>"
    }

    test{
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                struct_basic STRUCT<id: INT, name: STRING> MIN
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type MIN is not compatible with primitive type STRUCT<id:INT,name:TEXT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                struct_basic STRUCT<id: INT, name: STRING> MAX
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type MAX is not compatible with primitive type STRUCT<id:INT,name:TEXT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                struct_basic STRUCT<id: INT, name: STRING> HLL_UNION
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type HLL_UNION is not compatible with primitive type STRUCT<id:INT,name:TEXT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                struct_basic STRUCT<id: INT, name: STRING> BITMAP_UNION
            ) ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type BITMAP_UNION is not compatible with primitive type STRUCT<id:INT,name:TEXT>"
    }
     
    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT,
                struct_basic STRUCT<id: INT, name: STRING> QUANTILE_UNION
            ) ENGINE=OLAP   
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Aggregate type QUANTILE_UNION is not compatible with primitive type STRUCT<id:INT,name:TEXT>"
    }

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                struct_basic STRUCT<id: INT, name: STRING>,
                struct_value STRUCT<active: BOOLEAN, score: DOUBLE>
            ) ENGINE=OLAP
            DUPLICATE KEY(struct_basic)
            DISTRIBUTED BY HASH(struct_basic) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Struct can only be used in the non-key column of the duplicate table at present"
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            struct_basic STRUCT<id: INT, name: STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, struct_basic) VALUES (1, STRUCT(1, 'John')), (2, STRUCT(2, 'Jane')) """

    sql """ DROP TABLE IF EXISTS struct_table_1; """
    sql """
        CREATE TABLE IF NOT EXISTS struct_table_1 (
            id INT,
            struct_basic STRUCT<id: INT, name: STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO struct_table_1 (id, struct_basic) VALUES (1, STRUCT(1, 'John')), (2, STRUCT(2, 'Jane')) """

    sql """ DROP TABLE IF EXISTS struct_table_2; """
    sql """
        CREATE TABLE IF NOT EXISTS struct_table_2 (
            id INT,
            struct_basic STRUCT<id: INT, name: STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO struct_table_2 (id, struct_basic) VALUES (1, STRUCT(1, 'John')), (2, STRUCT(2, 'Jane')) """

    test {
        sql """ select * from struct_table_1 join struct_table_2 on struct_table_1.struct_basic = struct_table_2.struct_basic """
        exception "comparison predicate could not contains complex type: (struct_basic = struct_basic)"
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            struct_basic STRUCT<id: INT, name: STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, struct_basic) VALUES (1, STRUCT(1, 'John')), (2, STRUCT(2, 'Jane')), (3, STRUCT(3, 'Bob')) """

    test {
        sql """ DELETE FROM ${tableName} WHERE struct_element(struct_basic, 1) = 1 """
        exception "Can not apply delete condition to column type: struct<id:int,name:text>"
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            struct_basic STRUCT<id: INT, name: STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """ INSERT INTO ${tableName} (id, struct_basic) VALUES (1, STRUCT(1, 'John')), (2, STRUCT(2, 'Jane')), (3, STRUCT(3, 'Bob')) """

    qt_sql """ select struct_element(struct_basic, 1), struct_element(struct_basic, 2), struct_element(struct_basic, 'id'), struct_element(struct_basic, 'name') from ${tableName} order by id """

    qt_sql """ SELECT STRUCT_ELEMENT(struct_basic, 1), STRUCT_ELEMENT(struct_basic, 'name') FROM ${tableName} order by id """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            struct_basic STRUCT<id: INT, name: STRING, active: BOOLEAN>,
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ insert into ${tableName} values (1, STRUCT(1, 'John', true)), (2, STRUCT(2, 'Jane', false)), (3, STRUCT(3, 'Bob', true)); """

    test {
        sql """ delete from ${tableName} where struct_element(struct_basic, 'active') = true; """
        exception "Can not apply delete condition to column type: struct<id:int,name:text,active:boolean>"
    }

    qt_sql """ SELECT STRUCT(1, 'Alice', true);"""

    qt_sql """ SELECT STRUCT(1, 'Alice', true) as person;"""

    qt_sql """ SELECT STRUCT_ELEMENT(STRUCT(1, 'Alice', true), 1); """

    qt_sql """ SELECT STRUCT_ELEMENT(STRUCT(1, 'Alice', true), 2);"""
    
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            struct_nested STRUCT<person: STRUCT<name: STRING, age: INT>, address: STRUCT<city: STRING, zip: INT>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES (1, STRUCT(STRUCT('John', 25), STRUCT('New York', 10001)))"""
    sql """ INSERT INTO ${tableName} VALUES (2, STRUCT(STRUCT('Jane', 30), STRUCT('Los Angeles', 90001))) """

    qt_sql """ SELECT STRUCT_ELEMENT(STRUCT_ELEMENT(struct_nested, 'person'), 'name'), STRUCT_ELEMENT(STRUCT_ELEMENT(struct_nested, 'address'), 'city') FROM ${tableName} order by id """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            struct_array STRUCT<id: INT, tags: ARRAY<STRING>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES (1, STRUCT(1, ['tag1', 'tag2', 'tag3'])), (2, STRUCT(2, ['tag4', 'tag5'])) """

    qt_sql """ SELECT STRUCT_ELEMENT(struct_array, 'id'), STRUCT_ELEMENT(struct_array, 'tags') FROM ${tableName} order by id """
    
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            struct_map STRUCT<id: INT, attributes: MAP<STRING, INT>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES (1, STRUCT(1, MAP('key1', 1, 'key2', 2))) """

    qt_sql """ SELECT STRUCT_ELEMENT(struct_map, 'id'), STRUCT_ELEMENT(struct_map, 'attributes') FROM ${tableName} order by id """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `k` INT NOT NULL,
            `struct_varchar` STRUCT<name: VARCHAR(10), age: INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ ALTER TABLE ${tableName} MODIFY COLUMN struct_varchar STRUCT<name: VARCHAR(20), age: INT>; """

    qt_sql """ DESC ${tableName} """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            struct_complex STRUCT<
                basic_info: STRUCT<name: STRING, age: INT>,
                contact: STRUCT<email: STRING, phone: STRING>,
                preferences: STRUCT<tags: ARRAY<STRING>, settings: MAP<STRING, INT>>,
                metadata: STRUCT<
                    created_at: DATETIME,
                    updated_at: DATETIME,
                    stats: STRUCT<views: INT, clicks: INT>
                >
            >
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES (1, STRUCT(
        STRUCT('John', 25),
        STRUCT('john@example.com', '1234567890'),
        STRUCT(['tag1', 'tag2'], MAP('setting1', 1, 'setting2', 2)),
        STRUCT('2021-01-01 00:00:00', '2021-01-02 00:00:00', STRUCT(100, 50))
    )) """

    qt_sql """ SELECT 
        STRUCT_ELEMENT(STRUCT_ELEMENT(struct_complex, 'basic_info'), 'name'),
        STRUCT_ELEMENT(STRUCT_ELEMENT(struct_complex, 'contact'), 'email'),
        STRUCT_ELEMENT(STRUCT_ELEMENT(STRUCT_ELEMENT(struct_complex, 'metadata'), 'stats'), 'views')
    FROM ${tableName} order by id """

    test {
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `k` INT NOT NULL,
                `struct_varchar` STRUCT<name: VARCHAR(10), age: INT>,
                INDEX struct_varchar_index (struct_varchar) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "STRUCT<StructField ( name=name, dataType=VARCHAR(10), nullable=true ),StructField ( name=age, dataType=INT, nullable=true )> is not supported in INVERTED index. invalid index: struct_varchar_index"
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `k` INT NOT NULL,
            `struct_varchar` STRUCT<name: VARCHAR(10), age: INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql """ ALTER TABLE ${tableName} MODIFY COLUMN struct_varchar STRUCT<name: VARCHAR(10), age: INT, id: INT>; """

    qt_sql """ DESC ${tableName}; """
}
