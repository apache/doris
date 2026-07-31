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

suite("test_iceberg_multi_spec_filter_aggregate", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalogName = "test_iceberg_multi_spec_filter_aggregate"
    String dbName = "multi_spec_filter_aggregate_db"
    String tableName = "multi_spec_filter_aggregate"
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
                code string,
                metric int
            )
        """
        sql """insert into ${tableName} values (1, 'aa-01', 10), (11, 'bb-11', 110)"""

        sql """alter table ${tableName} add partition key bucket(4, id)"""
        sql """insert into ${tableName} values (2, 'aa-02', 20), (12, 'bb-12', 120)"""

        sql """alter table ${tableName} add partition key truncate(2, code)"""
        sql """insert into ${tableName} values (3, 'aa-03', 30), (13, 'bb-13', 130)"""

        sql """alter table ${tableName} drop partition key bucket(4, id)"""
        sql """insert into ${tableName} values (4, 'aa-04', 40), (14, 'bb-14', 140)"""

        // Every data file must be evaluated with the partition spec that wrote it; applying the
        // newest transform to old-spec files can silently prune valid rows or double-count keys.
        order_qt_multi_spec_rows """
            select id, code, metric from ${tableName} order by id
        """
        order_qt_multi_spec_static_filter """
            select id, code
            from ${tableName}
            where id in (1, 3, 12, 14) and code >= 'aa-00'
            order by id
        """
        order_qt_multi_spec_runtime_filter """
            with filter_keys as (
                select 2 as id
                union all
                select 3
                union all
                select 12
                union all
                select 14
            )
            select t.id, t.code, t.metric
            from ${tableName} t
            join filter_keys k on t.id = k.id
            order by t.id
        """
        order_qt_multi_spec_grouped """
            select substr(code, 1, 2) as code_prefix,
                   count(*) as row_count,
                   count(distinct id) as distinct_ids,
                   sum(metric) as metric_sum
            from ${tableName}
            group by code_prefix
            order by code_prefix
        """
        qt_multi_spec_distinct """
            select count(*), count(distinct id), count(distinct code)
            from ${tableName}
        """
        order_qt_multi_spec_metadata """
            select spec_id, sum(record_count)
            from ${tableName}\$partitions
            group by spec_id
            order by spec_id
        """
    } finally {
        sql """drop database if exists ${catalogName}.${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
