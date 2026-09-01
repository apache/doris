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

suite("test_lance_index_ddl", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance index DDL test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String lanceRestPort = context.config.otherConfigs.get("lance_rest_port")
    String filesystemCatalog = "test_lance_index_ddl"
    String restCatalog = "test_lance_index_ddl_rest"
    String user = "test_lance_index_ddl_user"
    String password = "C123_567p"

    sql """DROP CATALOG IF EXISTS `${filesystemCatalog}`"""
    sql """DROP CATALOG IF EXISTS `${restCatalog}`"""
    try_sql "DROP USER '${user}'@'%'"

    try {
        sql """
            CREATE CATALOG `${filesystemCatalog}` PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "filesystem",
                "warehouse" = "s3://warehouse/lance",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "use_path_style" = "true"
            )
        """

        // doris.vs_ivf_pq_f32 schema (all NOT NULL): embedding array<float>, row_id bigint,
        // category text, label text. Statically valid index DDL passes the section 2.4 matrix
        // and is then uniformly rejected until the Lance index build path lands.
        test {
            sql """CREATE INDEX idx ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32` (embedding) USING ANN
                   PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="256", "num_sub_vectors"="16")"""
            exception "CREATE INDEX is not supported for Lance catalog tables"
        }

        test {
            sql """CREATE INDEX idx ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32` (embedding) USING ANN
                   PROPERTIES("index_type"="IVF_PQ", "metric"="l1", "num_partitions"="256", "num_sub_vectors"="16")"""
            exception "metric must be one of l2, cosine, dot"
        }

        test {
            sql """CREATE INDEX idx ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32` (embedding) USING ANN
                   PROPERTIES("index_type"="IVF_PQ", "num_sub_vectors"="16")"""
            exception "num_partitions must be a positive integer"
        }

        test {
            sql """CREATE INDEX idx ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32` (row_id) USING BTREE"""
            exception "CREATE INDEX is not supported for Lance catalog tables"
        }

        test {
            sql """CREATE INDEX idx ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32` (row_id) USING BTREE
                   PROPERTIES("k"="v")"""
            exception "BTREE indexes do not support properties"
        }

        test {
            sql """CREATE INDEX idx ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32` (category) USING BITMAP"""
            exception "CREATE INDEX is not supported for Lance catalog tables"
        }

        test {
            sql """CREATE OR REPLACE INDEX idx ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32` (row_id) USING BTREE"""
            exception "CREATE OR REPLACE INDEX is not supported for Lance catalog tables"
        }

        test {
            sql """CREATE OR REPLACE INDEX IF NOT EXISTS idx ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32` (row_id) USING BTREE"""
            exception "[OR REPLACE] and [IF NOT EXISTS] cannot used at the same time"
        }

        test {
            sql """DROP INDEX idx ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32`"""
            exception "DROP INDEX is not supported for Lance catalog tables"
        }

        // Reject-all mode is uniform: IF EXISTS does not change the outcome.
        test {
            sql """DROP INDEX IF EXISTS idx ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32`"""
            exception "DROP INDEX is not supported for Lance catalog tables"
        }

        // An empty backquoted index name is a blank name, not an unsupported operation.
        test {
            sql """CREATE INDEX `` ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32` (embedding) USING ANN
                   PROPERTIES("index_type"="IVF_PQ", "num_partitions"="256", "num_sub_vectors"="16")"""
            exception "index name cannot be empty"
        }

        test {
            sql """DROP INDEX `` ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32`"""
            exception "index name cannot be empty"
        }

        // ALTER TABLE ADD/DROP INDEX keeps the generic external-table rejection.
        test {
            sql """ALTER TABLE `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32` ADD INDEX idx (category) USING INVERTED"""
            exception "do not support SCHEMA_CHANGE clause now"
        }

        sql """
            CREATE CATALOG `${restCatalog}` PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "rest",
                "lance.rest.uri" = "http://${externalEnvIp}:${lanceRestPort}",
                "lance.rest.security.type" = "bearer",
                "lance.rest.bearer-token" = "doris-lance-rest-test-token",
                "lance.namespace.root_database" = "default",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.region" = "us-east-1",
                "use_path_style" = "true",
                "test_connection" = "true"
            )
        """

        test {
            sql """CREATE INDEX idx ON `${restCatalog}`.`default`.`all_types` (row_id) USING BTREE"""
            exception "CREATE INDEX is not supported for Lance REST catalogs"
        }

        test {
            sql """CREATE OR REPLACE INDEX idx ON `${restCatalog}`.`default`.`all_types` (row_id) USING BTREE"""
            exception "CREATE OR REPLACE INDEX is not supported for Lance REST catalogs"
        }

        test {
            sql """DROP INDEX idx ON `${restCatalog}`.`default`.`all_types`"""
            exception "DROP INDEX is not supported for Lance REST catalogs"
        }

        sql """CREATE USER '${user}'@'%' IDENTIFIED BY '${password}'"""
        sql """GRANT SELECT_PRIV ON regression_test TO '${user}'@'%'"""
        if (isCloudMode()) {
            def clusters = sql "SHOW CLUSTERS"
            assertTrue(!clusters.isEmpty())
            sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO '${user}'@'%'"""
        }

        connect(user, password, context.config.jdbcUrl) {
            // The ALTER privilege check precedes the typed Lance rejection.
            test {
                sql """CREATE INDEX idx ON `${filesystemCatalog}`.`doris`.`vs_ivf_pq_f32` (row_id) USING BTREE"""
                exception "denied"
            }
        }
    } finally {
        try_sql "DROP USER '${user}'@'%'"
        // Keep both catalogs for debugging when the suite fails.
    }
}
