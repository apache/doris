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

suite("test_lance_index_entries", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance index entries test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String lanceRestPort = context.config.otherConfigs.get("lance_rest_port")
    String filesystemCatalog = "test_lance_index_entries"
    String restCatalog = "test_lance_index_entries_rest"
    String user = "test_lance_index_entries_user"
    String password = "C123_567p"

    // Index UUIDs are fixture-build artifacts; assert the shape instead of the value.
    String uuidShape = "'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\$'"

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

        order_qt_entries_vector """
            SELECT CatalogName, DatabaseName, TableName, IndexName,
                   IF(IndexUuid REGEXP ${uuidShape}, 'UUID', CONCAT('BAD:', IndexUuid)) AS UuidShape,
                   IF(DatasetVersion >= 1, 'SET', 'BAD') AS VersionState
            FROM lance_index_entries("table" = "${filesystemCatalog}.doris.vs_ivf_pq_f32")
        """

        qt_entries_vector_count """
            SELECT COUNT(*), COUNT(DISTINCT IndexName), COUNT(DISTINCT IndexUuid)
            FROM lance_index_entries("table" = "${filesystemCatalog}.doris.vs_ivf_pq_f32")
        """

        order_qt_entries_nested """
            SELECT CatalogName, DatabaseName, TableName, IndexName,
                   IF(IndexUuid REGEXP ${uuidShape}, 'UUID', CONCAT('BAD:', IndexUuid)) AS UuidShape,
                   IF(DatasetVersion >= 1, 'SET', 'BAD') AS VersionState
            FROM lance_index_entries("table" = "${filesystemCatalog}.doris.nested_index")
        """

        qt_entries_no_indexes """
            SELECT COUNT(*) FROM lance_index_entries("table" = "${filesystemCatalog}.doris.predicate_pushdown")
        """

        // An ordinary predicate filters the bounded result.
        qt_entries_predicate """
            SELECT IndexName FROM lance_index_entries("table" = "${filesystemCatalog}.doris.vs_ivf_pq_f32")
            WHERE IndexName = "no_such_index"
        """

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
            sql """SELECT * FROM lance_index_entries("table" = "${restCatalog}.`default`.all_types")"""
            exception "lance_index_entries is not supported for Lance REST catalogs"
        }

        sql """CREATE USER '${user}'@'%' IDENTIFIED BY '${password}'"""
        sql """GRANT SELECT_PRIV ON regression_test TO '${user}'@'%'"""
        if (isCloudMode()) {
            def clusters = sql "SHOW CLUSTERS"
            assertTrue(!clusters.isEmpty())
            sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO '${user}'@'%'"""
        }

        connect(user, password, context.config.jdbcUrl) {
            test {
                sql """SELECT * FROM lance_index_entries("table" = "${filesystemCatalog}.doris.vs_ivf_pq_f32")"""
                exception "denied"
            }
        }
    } finally {
        try_sql "DROP USER '${user}'@'%'"
        // Keep both catalogs for debugging when the suite fails.
    }
}
