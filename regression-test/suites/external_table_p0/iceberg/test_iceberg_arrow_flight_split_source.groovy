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

// Regression for https://github.com/apache/doris/issues/62259
//
// Querying an Iceberg external table over Arrow Flight SQL in batch split mode used to fail
// (BE crash / "Split source X is released"). Arrow Flight executes a query in two phases:
// GetFlightInfo (plan + submit to BE) then DoGet (the client pulls results from the BE). In
// batch split mode the BE keeps scanning during DoGet and lazily fetches file splits from the
// FE via the fetchSplitBatch RPC, using an async SplitSource that the FE coordinator holds. The
// FE used to release that SplitSource at the end of GetFlightInfo, before the BE's DoGet, so the
// split fetch failed. The MySQL protocol is unaffected because plan + execute share one request.
//
// This test forces batch split mode on the Arrow Flight session and scans the table, which must
// now return all rows.
suite("test_iceberg_arrow_flight_split_source", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    // The bug only manifests over the Arrow Flight SQL protocol. Skip when it is not configured.
    String arrowFlightHost = context.config.otherConfigs.get("extArrowFlightSqlHost")
    if (arrowFlightHost == null || arrowFlightHost.isEmpty()) {
        logger.info("extArrowFlightSqlHost is not configured, skip the test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_arrow_flight_split_source"
    // sample_cow_orc has 1000 rows; with num_files_in_batch_mode=1 a plain scan uses batch mode.
    String table = "${catalog_name}.format_v2.sample_cow_orc"

    sql """drop catalog if exists ${catalog_name}"""
    sql """CREATE CATALOG ${catalog_name} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${rest_port}',
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
            "s3.region" = "us-east-1"
        );"""

    try {
        // Baseline over the MySQL protocol (works regardless of the bug).
        def expected = sql """ select count(*) from ${table}; """
        long expectedRows = (expected[0][0] as long)
        assert expectedRows > 0 : "precondition: ${table} should not be empty"

        // Force batch split mode on the Arrow Flight session (a separate session from the MySQL
        // connection above, so the variables must be set here). With num_files_in_batch_mode=1
        // even a single-file scan builds the async SplitSource that triggers #62259.
        arrow_flight_sql """ set enable_external_table_batch_mode = true; """
        arrow_flight_sql """ set num_files_in_batch_mode = 1; """

        // Make sure the Arrow Flight session really uses the batch SplitSource path, so the test
        // cannot silently pass on the non-batch path. "approximate" only appears in batch mode.
        def explainRows = arrow_flight_sql """ explain select * from ${table}; """
        boolean isBatch = explainRows.any { row ->
            row.any { cell -> cell != null && cell.toString().contains("approximate") }
        }
        assert isBatch : "expected batch split mode (approximate) in the Arrow Flight plan, got: ${explainRows}"

        // The regression: a real data scan over Arrow Flight SQL (not count(*), which is pushed
        // down and bypasses batch mode). Before the fix this failed with "Split source X is
        // released" or crashed the BE; now it must return all rows.
        def flightResult = arrow_flight_sql """ select * from ${table}; """
        assertEquals(expectedRows, (flightResult.size() as long))

        // A second scan on the same connection also exercises cleanup of the previous query's
        // deferred coordinator when the next query starts.
        def flightLimited = arrow_flight_sql """ select * from ${table} limit 10; """
        assert flightLimited.size() > 0 && flightLimited.size() <= 10 : "unexpected row count: ${flightLimited.size()}"
    } finally {
        arrow_flight_sql """ set num_files_in_batch_mode = 1024; """
        sql """drop catalog if exists ${catalog_name}"""
    }
}
