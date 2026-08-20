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

suite("test_iceberg_schema_evolution_filter_binding", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalogName = "test_iceberg_schema_evolution_filter_binding"
    String dbName = "schema_evolution_filter_binding_db"
    String tableName = "schema_evolution_filter_binding"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type' = 'iceberg',
            'iceberg.catalog.type' = 'rest',
            'uri' = 'http://${externalEnvIp}:${restPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.region' = 'us-east-1'
        )
    """

    try {
        sql """switch ${catalogName}"""
        sql """drop database if exists ${dbName} force"""
        sql """create database ${dbName}"""
        sql """use ${dbName}"""
        sql """set enable_fallback_to_original_planner = false"""
        sql """set runtime_filter_mode = 'GLOBAL'"""

        sql """
            create table ${tableName} (
                id int,
                old_name string,
                dropped_value string,
                metric int
            )
        """
        sql """
            insert into ${tableName} values
                (1, 'old-a', 'drop-a', 10),
                (2, 'old-b', 'drop-b', 20),
                (3, 'old-c', 'drop-c', 30)
        """

        // Warm the scan metadata before changing field names and types. Predicates must continue
        // to bind by Iceberg field ID rather than by a cached ordinal from the old schema.
        order_qt_schema_filter_warm """
            select id, old_name, metric from ${tableName} order by id
        """
        sql """alter table ${tableName} rename column old_name new_name"""
        sql """alter table ${tableName} drop column dropped_value"""
        sql """alter table ${tableName} modify column metric bigint"""
        sql """
            insert into ${tableName} values
                (4, 'new-d', 4000000000),
                (5, 'new-e', 5000000000)
        """

        order_qt_schema_filter_static """
            select id, new_name, metric
            from ${tableName}
            where new_name in ('old-b', 'new-d') and metric >= 20
            order by id
        """
        order_qt_schema_filter_runtime """
            with filter_keys as (
                select 2 as id
                union all
                select 4
            )
            select t.id, t.new_name, t.metric
            from ${tableName} t
            join filter_keys k on t.id = k.id
            order by t.id
        """
        qt_schema_filter_aggregate """
            with filter_keys as (
                select 'old-b' as new_name
                union all
                select 'new-e'
            )
            select count(*), sum(t.metric)
            from ${tableName} t
            join filter_keys k on t.new_name = k.new_name
        """
        order_qt_schema_filter_null_semantics """
            select id, new_name
            from ${tableName}
            where new_name is not null and metric between 20 and 4000000000
            order by id
        """
    } finally {
        sql """drop database if exists ${catalogName}.${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
