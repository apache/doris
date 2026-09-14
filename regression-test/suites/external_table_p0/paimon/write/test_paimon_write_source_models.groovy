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

suite("test_paimon_write_source_models", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_source_models_catalog"
    String dbName = "test_pw_source_models_db"
    String internalDb = "test_pw_source_models_internal_db"

    sql """drop database if exists internal.${internalDb} force"""
    sql """create database internal.${internalDb}"""

    // Keep the source layouts deliberately different. The sink must consume the
    // source query result, not raw source rows hidden by each OLAP table model.
    sql """
        create table internal.${internalDb}.source_duplicate (
            id int,
            category varchar(20),
            amount bigint
        )
        duplicate key(id)
        distributed by random buckets 3
        properties ("replication_num" = "1")
    """
    sql """
        insert into internal.${internalDb}.source_duplicate values
            (1, 'A', 10),
            (1, 'A', 11),
            (2, null, 20)
    """

    sql """
        create table internal.${internalDb}.source_unique_mow (
            id int,
            category varchar(20),
            amount bigint
        )
        unique key(id, category)
        partition by list(category) (
            partition p_ab values in ('A', 'B'),
            partition p_null values in (null)
        )
        distributed by hash(id) buckets auto
        properties (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        insert into internal.${internalDb}.source_unique_mow values
            (10, 'A', 100), (11, null, 110)
    """
    sql """
        insert into internal.${internalDb}.source_unique_mow values
            (10, 'A', 101)
    """

    sql """
        create table internal.${internalDb}.source_unique_mor (
            id int,
            category varchar(20),
            amount bigint
        )
        unique key(id)
        partition by range(id) (
            partition p_lt_20 values less than (20),
            partition p_max values less than maxvalue
        )
        distributed by hash(id) buckets 2
        properties (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        )
    """
    sql """
        insert into internal.${internalDb}.source_unique_mor values
            (20, 'C', 200), (21, 'D', 210)
    """
    sql """
        insert into internal.${internalDb}.source_unique_mor values
            (20, 'C', 201)
    """

    sql """
        create table internal.${internalDb}.source_aggregate (
            id int,
            category varchar(20),
            amount bigint sum
        )
        aggregate key(id, category)
        partition by range(id) (
            partition p_lt_40 values less than (40),
            partition p_max values less than maxvalue
        )
        distributed by hash(id, category) buckets 4
        properties ("replication_num" = "1")
    """
    sql """
        insert into internal.${internalDb}.source_aggregate values
            (30, 'E', 300),
            (30, 'E', 3),
            (31, 'F', 310)
    """

    sql """
        create table internal.${internalDb}.source_complex (
            id int,
            metrics array<decimal(10, 2)>,
            attributes map<string, int>,
            profile struct<name:string, active:boolean>,
            flags array<boolean>,
            nested_payload map<string, array<struct<score:int, label:string>>>,
            event_date date,
            event_time datetime(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 3
        properties ("replication_num" = "1")
    """
    sql """
        insert into internal.${internalDb}.source_complex values
            (
                1,
                array(cast(1.25 as decimal(10, 2)), cast(null as decimal(10, 2))),
                map('alpha', 10, 'nullable', null),
                named_struct('name', 'alice', 'active', true),
                array(true, false, cast(null as boolean)),
                map('term', array(
                    named_struct('score', 90, 'label', 'good'),
                    named_struct('score', cast(null as int), 'label', null)
                )),
                date '2024-02-29',
                timestamp '2024-02-29 12:34:56.123456'
            ),
            (
                2,
                array(),
                map(),
                named_struct('name', cast(null as string),
                             'active', cast(null as boolean)),
                array(),
                map('empty', array()),
                date '1970-01-01',
                timestamp '1970-01-01 00:00:00.000001'
            ),
            (3, null, null, null, null, null, null, null)
    """

    spark_paimon_multi """
        SET spark.sql.timestampType=TIMESTAMP_NTZ;
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.source_model_sink;
        CREATE TABLE paimon.${dbName}.source_model_sink (
            source_model STRING NOT NULL,
            id INT,
            category STRING,
            amount BIGINT
        ) USING paimon
        PARTITIONED BY (source_model)
        TBLPROPERTIES ('file.format' = 'parquet');

        DROP TABLE IF EXISTS paimon.${dbName}.complex_sink;
        CREATE TABLE paimon.${dbName}.complex_sink (
            id INT,
            metrics ARRAY<DECIMAL(10, 2)>,
            attributes MAP<STRING, INT>,
            profile STRUCT<name:STRING, active:BOOLEAN>,
            flags ARRAY<BOOLEAN>,
            nested_payload MAP<STRING, ARRAY<STRUCT<score:INT, label:STRING>>>,
            event_date DATE,
            event_time TIMESTAMP_NTZ
        ) USING paimon
        TBLPROPERTIES ('file.format' = 'orc');
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        )
    """
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    try {
        sql """
            insert into source_model_sink
            select 'duplicate', id, category, amount
            from internal.${internalDb}.source_duplicate
        """
        sql """
            insert into source_model_sink
            select 'unique_mow', id, category, amount
            from internal.${internalDb}.source_unique_mow
        """
        sql """
            insert into source_model_sink
            select 'unique_mor', id, category, amount
            from internal.${internalDb}.source_unique_mor
        """
        sql """
            insert into source_model_sink
            select 'aggregate', id, category, amount
            from internal.${internalDb}.source_aggregate
        """

        def sourceRows = sql """
            select 'duplicate', id, category, amount
            from internal.${internalDb}.source_duplicate
            union all
            select 'unique_mow', id, category, amount
            from internal.${internalDb}.source_unique_mow
            union all
            select 'unique_mor', id, category, amount
            from internal.${internalDb}.source_unique_mor
            union all
            select 'aggregate', id, category, amount
            from internal.${internalDb}.source_aggregate
            order by 1, 2, 3, 4
        """
        def sinkRows = sql """
            select source_model, id, category, amount
            from source_model_sink
            order by 1, 2, 3, 4
        """
        assertEquals(sourceRows, sinkRows)
        assertEquals(4L,
                (sql """select count(*) from source_model_sink\$snapshots""")[0][0] as long)

        def sparkModelRows = spark_paimon """
            select source_model, id, category, amount
            from paimon.${dbName}.source_model_sink
            order by source_model, id, category, amount
        """
        assertSparkDorisResultEquals(sparkModelRows, sinkRows)

        // Complex values now cross the OLAP scanner and an INSERT SELECT
        // projection before reaching the Paimon Arrow writer.
        sql """
            insert into complex_sink
            select id, metrics, attributes, profile, flags, nested_payload,
                   event_date, event_time
            from internal.${internalDb}.source_complex
        """
        def complexRows = sql """
            select id, metrics, attributes, profile, flags, nested_payload,
                   event_date, event_time
            from complex_sink
            order by id
        """
        def sparkComplexRows = spark_paimon """
            select id, metrics, attributes, profile, flags, nested_payload,
                   event_date, event_time
            from paimon.${dbName}.complex_sink
            order by id
        """
        assertSparkDorisResultEquals(sparkComplexRows, complexRows)
        assertEquals(3L, complexRows.size() as long)
        assertEquals(1L,
                (sql """select count(*) from complex_sink\$snapshots""")[0][0] as long)
    } finally {
        sql """switch internal"""
        sql """drop catalog if exists ${catalogName}"""
        sql """drop database if exists internal.${internalDb} force"""
    }
}
