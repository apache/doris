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
     * managed_versioning is false for all_types: the table's versions live in its own
     * _versions/ directory. all_types_managed is the same dataset described with
     * managed_versioning=true, which Doris refuses until it resolves versions through
     * the namespace.
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
    String restUri = "http://${externalEnvIp}:${lanceRestPort}"

    sql """DROP CATALOG IF EXISTS test_lance_rest_header_conflict"""
    test {
        sql """CREATE CATALOG test_lance_rest_header_conflict PROPERTIES (
            "type" = "lance", "lance.catalog.type" = "rest",
            "lance.rest.uri" = "${restUri}",
            "lance.rest.header.X-Tenant" = "old", "lance.rest.header.x-tenant" = "new")"""
        exception "Conflicting values for HTTP header 'x-tenant'"
    }

    // SQL escapes produce real control bytes that the native HTTP client cannot send.
    sql "SET sql_mode = ''"
    sql "DROP CATALOG IF EXISTS test_lance_rest_header_control"
    for (String control : ["\\b", "\\0", "\\Z", "\u007f"]) {
        for (String mode : ["none", "bearer", "api_key"]) {
            String property = mode == "none" ? "lance.rest.header.X-Tenant"
                    : (mode == "bearer" ? "lance.rest.bearer-token" : "lance.rest.api-key")
            String valueType = mode == "none" ? "header" : "credential"
            test {
                sql """CREATE CATALOG test_lance_rest_header_control PROPERTIES (
                    "type" = "lance", "lance.catalog.type" = "rest",
                    "lance.rest.uri" = "${restUri}", "lance.rest.security.type" = "${mode}",
                    "${property}" = "tenant${control}")"""
                exception "Invalid HTTP ${valueType} value in property '${property}'"
            }
        }
    }

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    sql """
        CREATE CATALOG `${catalogName}` PROPERTIES (
            "type" = "lance",
            "lance.catalog.type" = "rest",
            "lance.rest.uri" = "${restUri}",
            "lance.rest.security.type" = "bearer",
            "lance.rest.bearer-token" = "${bearerToken}",
            "lance.rest.header.X-Tenant" = "old",
            "lance.namespace.root_database" = "default",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1",
            "use_path_style" = "true",
            "test_connection" = "true"
        )
    """

    // ALTER retains case-sensitive property keys. Update the original spelling, or all aliases together.
    test {
        sql """ALTER CATALOG `${catalogName}` SET PROPERTIES ("lance.rest.header.x-tenant" = "new")"""
        exception "Conflicting values for HTTP header 'x-tenant'"
    }
    sql """ALTER CATALOG `${catalogName}` SET PROPERTIES ("lance.rest.header.X-Tenant" = "new")"""
    sql """ALTER CATALOG `${catalogName}` SET PROPERTIES ("lance.rest.header.x-tenant" = "new")"""

    // ALTER merges properties and cannot remove keys. Clear the inactive credential while
    // exercising every authentication transition, then read with the fixture's bearer token.
    test {
        sql """ALTER CATALOG `${catalogName}` SET PROPERTIES (
            "lance.rest.security.type" = "api_key", "lance.rest.api-key" = "test-api-key")"""
        exception "requires only 'lance.rest.api-key'"
    }
    sql """ALTER CATALOG `${catalogName}` SET PROPERTIES (
        "lance.rest.security.type" = "api_key",
        "lance.rest.bearer-token" = "", "lance.rest.api-key" = "test-api-key")"""
    sql """ALTER CATALOG `${catalogName}` SET PROPERTIES (
        "lance.rest.security.type" = "none", "lance.rest.api-key" = "")"""
    sql """ALTER CATALOG `${catalogName}` SET PROPERTIES (
        "lance.rest.security.type" = "bearer", "lance.rest.bearer-token" = "${bearerToken}")"""
    sql """ALTER CATALOG `${catalogName}` SET PROPERTIES (
        "lance.rest.security.type" = "none", "lance.rest.bearer-token" = "")"""
    sql """ALTER CATALOG `${catalogName}` SET PROPERTIES (
        "lance.rest.security.type" = "api_key", "lance.rest.api-key" = "test-api-key")"""
    sql """ALTER CATALOG `${catalogName}` SET PROPERTIES (
        "lance.rest.security.type" = "bearer",
        "lance.rest.api-key" = "", "lance.rest.bearer-token" = "${bearerToken}")"""

    order_qt_rest_databases """SHOW DATABASES FROM `${catalogName}`"""
    order_qt_rest_tables """SHOW TABLES FROM `${catalogName}`.`default`"""

    // No static access key or secret key is configured. This exercises the
    // DescribeTable credential-vending path from FE to the BE scan range.
    qt_rest_scan """
        SELECT count(*), count(DISTINCT row_id), min(row_id), max(row_id), sum(row_id)
        FROM `${catalogName}`.`default`.`all_types`
    """

    // The same dataset, described by a namespace that spells the vended credentials without
    // the aws_ prefix. Lance accepts either alias, so both have to reach the BE; a client that
    // recognizes only one silently scans with no credentials at all.
    qt_rest_scan_unprefixed_credentials """
        SELECT count(*), count(DISTINCT row_id), min(row_id), max(row_id), sum(row_id)
        FROM `${catalogName}`.`default`.`all_types_unprefixed`
    """

    String pushedQuery =
            """SELECT row_id FROM `${catalogName}`.`default`.`all_types` WHERE int32_col = 10 ORDER BY row_id"""
    explain {
        sql(pushedQuery)
        contains "lancePushdownPredicate="
        contains "int32_col"
        notContains "PREDICATES:"
    }
    qt_rest_predicate_pushdown pushedQuery

    test {
        sql """SELECT count(*) FROM `${catalogName}`.`default`.`all_types_managed`"""
        exception "has its versions managed by the namespace (managed_versioning), which is not supported yet"
    }

    String showCreate = sql("""SHOW CREATE CATALOG `${catalogName}`""")[0][1].toString()
    assertFalse(showCreate.contains(bearerToken))
}
