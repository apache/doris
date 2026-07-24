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

suite("test_iceberg_schema_dual_relation_matrix",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_schema_dual_relation_matrix"
    String dbName = "iceberg_schema_dual_relation_db"
    String tableName = "dual_schema_timeline"

    def latestSnapshotId = {
        List<List<Object>> rows = spark_iceberg """
            select snapshot_id
            from demo.${dbName}.${tableName}.snapshots
            order by committed_at desc
            limit 1
        """
        assertEquals(1, rows.size())
        return rows[0][0].toString()
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='http://${externalEnvIp}:${restPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.region'='us-east-1'
        )
    """

    try {
        spark_iceberg_multi """
            create database if not exists demo.${dbName};
            drop table if exists demo.${dbName}.${tableName};
            create table demo.${dbName}.${tableName} (
                id int,
                old_name string,
                info struct<added: int, keep: int>
            ) using iceberg
            tblproperties ('format-version'='2', 'write.format.default'='parquet');
            insert into demo.${dbName}.${tableName}
                values (1, 'old-1', named_struct('added', 10, 'keep', 11));
        """
        String oldSnapshot = latestSnapshotId()

        spark_iceberg_multi """
            alter table demo.${dbName}.${tableName} rename column old_name to new_name;
            alter table demo.${dbName}.${tableName}
                rename column info.added to renamed;
            insert into demo.${dbName}.${tableName}
                values (2, 'new-2', named_struct('renamed', 20, 'keep', 21));
        """
        String newSnapshot = latestSnapshotId()

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""

        // Scenario TC07-baseline: each historical relation resolves its own schema in isolation.
        assertEquals([[1, "old-1", 10]], sql("""
            select id, old_name, info.added
            from ${tableName} for version as of ${oldSnapshot}
            order by id
        """))
        assertEquals([[1, "old-1", 10], [2, "new-2", 20]], sql("""
            select id, new_name, info.renamed
            from ${tableName} for version as of ${newSnapshot}
            order by id
        """))

        // Scenario TC07-join negative contract:
        // two historical relations in one statement currently reuse the first schema.
        test {
            sql """
                select o.id, o.old_name, n.new_name
                from (
                    select id, old_name
                    from ${tableName} for version as of ${oldSnapshot}
                ) o
                join (
                    select id, new_name
                    from ${tableName} for version as of ${newSnapshot}
                ) n on o.id = n.id
                order by o.id
            """
            exception "Unknown column 'new_name'"
        }

        // Scenario TC07-reverse-join: binding must be independent of relation order.
        test {
            sql """
                select n.id, n.new_name, o.old_name
                from (
                    select id, new_name
                    from ${tableName} for version as of ${newSnapshot}
                ) n
                join (
                    select id, old_name
                    from ${tableName} for version as of ${oldSnapshot}
                ) o on n.id = o.id
                order by n.id
            """
            exception "Unknown column 'old_name'"
        }

        // Scenario TC07-union: top-level historical schemas stay relation-local.
        test {
            sql """
                select id, old_name as name_value
                from ${tableName} for version as of ${oldSnapshot}
                union all
                select id, new_name as name_value
                from ${tableName} for version as of ${newSnapshot}
                order by id, name_value
            """
            exception "Unknown column 'new_name'"
        }

        // Scenario TC07-nested-union: nested field lookup is also relation-local.
        test {
            sql """
                select id, info.added as nested_value
                from ${tableName} for version as of ${oldSnapshot}
                union all
                select id, info.renamed as nested_value
                from ${tableName} for version as of ${newSnapshot}
                order by id, nested_value
            """
            exception "No such struct field 'renamed'"
        }

        // Scenario TC07-CTE: CTE boundaries must not collapse snapshot schemas.
        test {
            sql """
                with old_ref as (
                    select id, old_name
                    from ${tableName} for version as of ${oldSnapshot}
                ), new_ref as (
                    select id, new_name
                    from ${tableName} for version as of ${newSnapshot}
                )
                select old_ref.id, old_ref.old_name, new_ref.new_name
                from old_ref join new_ref on old_ref.id = new_ref.id
                order by old_ref.id
            """
            exception "Unknown column 'new_name'"
        }

        // Scenario TC07-correlated-subquery: subqueries require an independent schema.
        test {
            sql """
                select o.id, o.old_name
                from ${tableName} for version as of ${oldSnapshot} o
                where exists (
                    select 1
                    from ${tableName} for version as of ${newSnapshot} n
                    where n.id = o.id and n.new_name is not null
                )
                order by o.id
            """
            exception "Unknown column 'new_name'"
        }
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
