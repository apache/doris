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

suite("test_fluss_review_boundaries", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String host = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String minioPort = context.config.otherConfigs.get("fluss_minio_port")
    String baseCatalog = "test_fluss_review_boundaries"
    String instantCatalog = "test_fluss_review_boundaries_tz"

    sql """drop catalog if exists ${baseCatalog}"""
    sql """
        create catalog ${baseCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${host}:${coordinatorPort}",
            "fluss.lake.paimon.s3.endpoint" = "http://${host}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin"
        )
    """
    sql """drop catalog if exists ${instantCatalog}"""
    sql """
        create catalog ${instantCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${host}:${coordinatorPort}",
            "fluss.lake.paimon.s3.endpoint" = "http://${host}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "enable.mapping.timestamp_tz" = "true",
            "fluss.union_read.mode" = "required"
        )
    """

    sql """switch ${baseCatalog}"""
    sql """use fluss_test"""
    sql """set enable_file_scanner_v2 = true"""
    sql """set time_zone = 'America/New_York'"""

    // Fluss permits this quoted partition key. FE must reject it before the
    // generic comma-delimited metadata misidentifies the ordinary columns.
    test {
        sql """select id from log_comma_part"""
        exception "partition column 'region,code' contains a comma"
    }

    // Both catalogs preserve distinct DST-overlap instants as TIMESTAMPTZ keys, even
    // without the deprecated mapping option, so required lake/tail merging must succeed.
    sql """set fluss_union_read_mode = 'required'"""
    order_qt_ltz_default_union """select name from lake_pk_ltz order by name"""
    sql """set fluss_union_read_mode = ''"""
    order_qt_ltz_fluss_fallback """select name from lake_pk_ltz order by name"""

    // TIMESTAMPTZ keeps the instant, so the lake and tail may be merged by key.
    order_qt_ltz_instant_union """
        select name from ${instantCatalog}.fluss_test.lake_pk_ltz order by name
    """

    sql """switch internal"""
}
