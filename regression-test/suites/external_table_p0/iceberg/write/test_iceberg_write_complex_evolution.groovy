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

suite("test_iceberg_write_complex_evolution",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_complex_evolution"
    String dbName = "iceberg_write_complex_evolution_db"

    def assertSparkMatchesDoris = {
        sql """refresh table ${dbName}.complex_evolution"""
        spark_iceberg """refresh table demo.${dbName}.complex_evolution"""
        def sparkRows = spark_iceberg """
            select id, group_key, arr, mp, payload
            from demo.${dbName}.complex_evolution
            order by id
        """
        def dorisRows = sql """
            select id, group_key, arr, mp, payload
            from complex_evolution
            order by id
        """
        assertSparkDorisResultEquals(sparkRows, dorisRows)
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1",
            "meta.cache.iceberg.table.ttl-second" = "0",
            "meta.cache.iceberg.schema.ttl-second" = "0"
        )
    """
    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""
    sql """set enable_fallback_to_original_planner = false"""

    sql """drop table if exists complex_evolution"""
    sql """
        create table complex_evolution (
            id int not null,
            group_key string not null,
            arr array<int>,
            mp map<string, int>,
            payload struct<
                metric:int,
                label:string,
                nested:struct<count:int, comment:string>
            >
        )
        partition by list (group_key) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "orc"
        )
    """

    // W02-S01: Write old-schema rows, including NULL collections, elements, values and children.
    sql """
        insert into complex_evolution values
            (1, 'A', array(1, null, 3), map('x', 10, 'null-value', null),
                struct(10, 'old-a', struct(1, null))),
            (2, 'N', null, map('x', null),
                struct(20, null, struct(null, 'old-null'))),
            (3, 'B', array(), map(), null)
    """
    String baseSnapshot = (sql """
        select snapshot_id from complex_evolution\$snapshots
        order by committed_at desc limit 1
    """)[0][0].toString()
    sql """alter table complex_evolution create tag complex_base as of version ${baseSnapshot}"""
    assertSparkMatchesDoris()

    // W02-S02: Promote every supported nested primitive and add STRUCT children.
    // The following write checks that Doris uses Iceberg field ids rather than child positions.
    sql """alter table complex_evolution modify column arr array<bigint>"""
    sql """alter table complex_evolution modify column mp map<string, bigint>"""
    sql """
        alter table complex_evolution modify column payload struct<
            metric:bigint,
            label:string,
            nested:struct<count:bigint, comment:string, score:double>,
            tags:array<string>,
            attributes:map<string, bigint>
        >
    """
    sql """alter table complex_evolution add partition key bucket(8, id) as id_bucket"""
    sql """alter table complex_evolution add partition key truncate(1, group_key) as group_prefix"""

    sql """
        insert into complex_evolution values
            (4, 'A1', array(cast(4000000000 as bigint), null),
                map('large', cast(5000000000 as bigint), 'null-value', null),
                struct(
                    cast(6000000000 as bigint),
                    'new-a',
                    struct(cast(7000000000 as bigint), 'nested-new', cast(7.5 as double)),
                    array('x', null, 'z'),
                    map('a', cast(8000000000 as bigint), 'b', null)
                )),
            (5, 'N2', array(null), null,
                struct(
                    cast(50 as bigint),
                    null,
                    struct(cast(5 as bigint), null, null),
                    null,
                    map('null-value', null)
                ))
    """

    // W02-S03: Current schema reads both old and new files without moving old child values.
    order_qt_complex_current """
        select id, group_key, arr, mp, payload
        from complex_evolution
        order by id
    """
    order_qt_complex_children """
        select id, payload.metric, payload.nested.count, payload.nested.score,
               payload.tags, payload.attributes
        from complex_evolution
        order by id
    """
    order_qt_complex_nulls """
        select id
        from complex_evolution
        where group_key is null
           or arr is null
           or mp is null
           or payload is null
           or payload.nested.score is null
        order by id
    """
    order_qt_complex_partition_specs """
        select spec_id, sum(record_count)
        from complex_evolution\$partitions
        group by spec_id
        order by spec_id
    """
    assertSparkMatchesDoris()

    // W02-S04: A pre-evolution tag binds the old files to their historical complex schema.
    order_qt_complex_base_tag """
        select id, arr, mp, payload.metric, payload.label,
               payload.nested.count, payload.nested.comment
        from complex_evolution@tag(complex_base)
        order by id
    """
}
