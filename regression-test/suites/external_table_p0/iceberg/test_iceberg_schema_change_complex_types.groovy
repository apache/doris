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

suite("test_iceberg_schema_change_complex_types", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_schema_change_complex_types"
    String db_name = "iceberg_schema_change_complex_types_db"
    String table_name = "iceberg_complex_modify"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalog_name};"""
    sql """drop database if exists ${db_name} force"""
    sql """create database ${db_name}"""
    sql """use ${db_name};"""

    sql """set enable_fallback_to_original_planner=false;"""
    sql """set show_column_comment_in_describe=true;"""

    sql """drop table if exists ${table_name}"""
    sql """
    CREATE TABLE ${table_name} (
        id INT NOT NULL,
        arr_i ARRAY<INT>,
        arr_f ARRAY<FLOAT>,
        mp_i MAP<INT, INT>,
        mp_f MAP<INT, FLOAT>,
        st STRUCT<
            si: INT,
            sf: FLOAT,
            sv: STRING COMMENT 'v1',
            sm: MAP<INT, INT> COMMENT 'm1',
            sa: ARRAY<INT> COMMENT 'a1'
        > NOT NULL COMMENT 'old struct'
    );"""

    sql """
    INSERT INTO ${table_name} VALUES (
        1,
        ARRAY(1, 2),
        ARRAY(CAST(1.1 AS FLOAT)),
        MAP(1, 10),
        MAP(1, CAST(1.1 AS FLOAT)),
        STRUCT(1, CAST(1.1 AS FLOAT), 'abc', MAP(1, 10), ARRAY(1, 2))
    )"""

    // Complex type default value only supports NULL
    test {
        sql """
        ALTER TABLE ${table_name} MODIFY COLUMN st STRUCT<
            si: INT,
            sf: FLOAT,
            sv: STRING COMMENT 'v1',
            sm: MAP<INT, INT> COMMENT 'm1',
            sa: ARRAY<INT> COMMENT 'a1'
        > DEFAULT 'x'"""
        exception "just support null"
    }

    // Cannot change nullable complex column to not null
    test {
        sql """ALTER TABLE ${table_name} MODIFY COLUMN arr_i ARRAY<INT> NOT NULL"""
        exception "Cannot change nullable column arr_i to not null"
    }

    // Map key type change is not allowed
    test {
        sql """ALTER TABLE ${table_name} MODIFY COLUMN mp_i MAP<BIGINT, INT>"""
        exception "Cannot change MAP key type"
    }

    // Complex type category change is not allowed
    test {
        sql """ALTER TABLE ${table_name} MODIFY COLUMN arr_i MAP<INT, INT>"""
        exception "Cannot change complex column type category"
    }

    // Struct field rename is not allowed
    test {
        sql """
        ALTER TABLE ${table_name} MODIFY COLUMN st STRUCT<
            si_rename: INT,
            sf: FLOAT,
            sv: STRING COMMENT 'v1',
            sm: MAP<INT, INT> COMMENT 'm1',
            sa: ARRAY<INT> COMMENT 'a1'
        > NOT NULL COMMENT 'old struct'"""
        exception "Cannot rename struct field"
    }

    // Struct field drop is not allowed
    test {
        sql """
        ALTER TABLE ${table_name} MODIFY COLUMN st STRUCT<
            si: INT,
            sf: FLOAT,
            sv: STRING COMMENT 'v1',
            sm: MAP<INT, INT> COMMENT 'm1'
        > NOT NULL COMMENT 'old struct'"""
        exception "Cannot reduce struct fields"
    }

    // Nested type downgrade is not allowed
    test {
        sql """ALTER TABLE ${table_name} MODIFY COLUMN arr_i ARRAY<SMALLINT>"""
        exception "Cannot change int to smallint in nested types"
    }

    // Array element type promotions
    sql """ALTER TABLE ${table_name} MODIFY COLUMN arr_i ARRAY<BIGINT>"""
    sql """ALTER TABLE ${table_name} MODIFY COLUMN arr_f ARRAY<DOUBLE>"""

    // Map value type promotions
    sql """ALTER TABLE ${table_name} MODIFY COLUMN mp_i MAP<INT, BIGINT>"""
    sql """ALTER TABLE ${table_name} MODIFY COLUMN mp_f MAP<INT, DOUBLE>"""

    // Struct field promotions, nested complex updates, add field, and comment updates
    sql """
    ALTER TABLE ${table_name} MODIFY COLUMN st STRUCT<
        si: BIGINT,
        sf: DOUBLE,
        sv: STRING COMMENT 'v2',
        sm: MAP<INT, BIGINT> COMMENT 'm2',
        sa: ARRAY<BIGINT> COMMENT 'a2',
        sn: STRING COMMENT 'new field',
        sn_map: MAP<INT, BIGINT> COMMENT 'new map',
        sn_arr: ARRAY<BIGINT> COMMENT 'new array'
    > NULL COMMENT 'new struct'"""

    // Insert new data after schema change and keep old data readable
    sql """
    INSERT INTO ${table_name} VALUES (
        2,
        ARRAY(10, 20, 30),
        ARRAY(CAST(2.2 AS DOUBLE)),
        MAP(2, 200),
        MAP(2, CAST(2.2 AS DOUBLE)),
        STRUCT(
            2,
            CAST(2.2 AS DOUBLE),
            'xyz',
            MAP(2, 200),
            ARRAY(10, 20),
            'new field value',
            MAP(3, 300),
            ARRAY(30, 40)
        )
    )"""

    def descRows = sql """DESC ${table_name}"""
    def normalizeType = { String s -> s.toLowerCase().replaceAll("\\s+", "") }
    def typeOf = { String name ->
        def row = descRows.find { it[0].toString().equalsIgnoreCase(name) }
        assertTrue(row != null, "column not found: ${name}")
        return normalizeType(row[1].toString())
    }

    assertTrue(typeOf("arr_i").contains("array<bigint>"), descRows.toString())
    assertTrue(typeOf("arr_f").contains("array<double>"), descRows.toString())
    assertTrue(typeOf("mp_i").contains("map<int,bigint>"), descRows.toString())
    assertTrue(typeOf("mp_f").contains("map<int,double>"), descRows.toString())

    def stType = typeOf("st")
    assertTrue(stType.contains("si:bigint"), stType)
    assertTrue(stType.contains("sf:double"), stType)
    assertTrue(stType.contains("sv:text"), stType)
    assertTrue(stType.contains("sm:map<int,bigint>"), stType)
    assertTrue(stType.contains("sa:array<bigint>"), stType)
    assertTrue(stType.contains("sn:text"), stType)
    assertTrue(stType.contains("sn_map:map<int,bigint>"), stType)
    assertTrue(stType.contains("sn_arr:array<bigint>"), stType)

    def stRow = descRows.find { it[0].toString().equalsIgnoreCase("st") }
    assertTrue(stRow.toString().toLowerCase().contains("new struct"), stRow.toString())
    assertTrue(stRow[2].toString().equalsIgnoreCase("YES"), stRow.toString())

    // Reorder columns should still work after complex type changes
    sql """ALTER TABLE ${table_name} ORDER BY (id, arr_i, mp_i, st, arr_f, mp_f)"""

    def queryRes = sql """SELECT id,
                                 STRUCT_ELEMENT(st, 'si') AS si,
                                 STRUCT_ELEMENT(st, 'sn') AS sn,
                                 ARRAY_SIZE(arr_i) AS arr_sz
                         FROM ${table_name} ORDER BY id"""
    assertTrue(queryRes.size() == 2, queryRes.toString())
    assertTrue(queryRes[0][0].toString() == "1", queryRes.toString())
    assertTrue(queryRes[0][1].toString() == "1", queryRes.toString())
    assertTrue(queryRes[0][2] == null, queryRes.toString())
    assertTrue(queryRes[0][3].toString() == "2", queryRes.toString())

    assertTrue(queryRes[1][0].toString() == "2", queryRes.toString())
    assertTrue(queryRes[1][1].toString() == "2", queryRes.toString())
    assertTrue(queryRes[1][2].toString() == "new field value", queryRes.toString())
    assertTrue(queryRes[1][3].toString() == "3", queryRes.toString())

    sql """drop table if exists ${table_name}"""
    sql """drop database if exists ${db_name} force"""
    sql """drop catalog if exists ${catalog_name}"""
}
