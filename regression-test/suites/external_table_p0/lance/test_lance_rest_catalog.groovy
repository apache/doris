// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to You under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

suite("test_lance_rest_catalog", "p0,external") {
    /*
     * The Docker Lance REST service owns namespace metadata, while the table is
     * the real all_types.lance dataset preinstalled in MinIO. This covers:
     *
     *   FE Lance REST Namespace
     *     -> ListNamespaces / ListTables / DescribeTable
     *     -> vended S3 credentials
     *     -> FE pins the Lance dataset version and fragments
     *     -> BE lance-c reads the real dataset from MinIO
     *
     * managed_versioning is false because the current BE opens storage-native
     * Lance versions by URI and does not resolve REST-managed external manifests.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance REST test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String lanceRestPort = context.config.otherConfigs.get("lance_rest_port")
    String catalogName = "test_lance_rest_catalog"
    String bearerToken = "doris-lance-rest-test-token"
    String tableName = "all_types"
    String restUri = "http://${externalEnvIp}:${lanceRestPort}"

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    try {
        sql """
            CREATE CATALOG `${catalogName}` PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "rest",
                "lance.rest.uri" = "${restUri}",
                "lance.rest.security.type" = "bearer",
                "lance.rest.bearer-token" = "${bearerToken}",
                "lance.namespace.root_database" = "default",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.region" = "us-east-1",
                "use_path_style" = "true",
                "test_connection" = "true"
            )
        """

        order_qt_rest_databases """SHOW DATABASES FROM `${catalogName}`"""
        order_qt_rest_tables """SHOW TABLES FROM `${catalogName}`.`default`"""

        // No static access key or secret key is configured. This exercises the
        // DescribeTable credential-vending path from FE to the BE scan range.
        qt_rest_scan """
            SELECT count(*), count(DISTINCT row_id), min(row_id), max(row_id), sum(row_id)
            FROM `${catalogName}`.`default`.`${tableName}`
        """

        // The same dataset, described by a namespace that spells the vended credentials without
        // the aws_ prefix. Lance accepts either alias, so both have to reach the BE; a client that
        // recognizes only one silently scans with no credentials at all.
        qt_rest_scan_unprefixed_credentials """
            SELECT count(*), count(DISTINCT row_id), min(row_id), max(row_id), sum(row_id)
            FROM `${catalogName}`.`default`.`all_types_unprefixed`
        """

        String pushedQuery =
                """SELECT row_id FROM `${catalogName}`.`default`.`${tableName}` WHERE int32_col = 10 ORDER BY row_id"""
        explain {
            sql(pushedQuery)
            contains "lancePushdownPredicate="
            contains "int32_col"
            notContains "predicates:"
        }
        qt_rest_predicate_pushdown pushedQuery

        // A pushed Lance predicate must disable the unfiltered Fragment metadata count.
        String filteredCountQuery =
                """SELECT count(*) FROM `${catalogName}`.`default`.`${tableName}` WHERE int32_col = 10"""
        explain {
            sql(filteredCountQuery)
            contains "pushdown agg=COUNT (-1)"
            contains "lancePushdownPredicate="
        }
        qt_rest_filtered_count filteredCountQuery

        String showCreate = sql("""SHOW CREATE CATALOG `${catalogName}`""")[0][1].toString()
        assertFalse(showCreate.contains(bearerToken))
    } finally {
        // sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
