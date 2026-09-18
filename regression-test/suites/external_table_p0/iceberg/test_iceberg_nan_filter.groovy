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

// DORIS-29047. Doris orders NaN above every other floating-point value, so `d > 5` and `d >= 5` are true for a
// NaN row. Iceberg's file metrics take the opposite view: NaN is excluded from the lower/upper bounds and an
// all-NaN column is flagged via nan_value_counts, so pushing a bare greaterThan prunes files that do hold
// matching rows -- BE never sees the split and the rows vanish silently. The fix ORs an is_nan arm into the
// pushed range predicate; the inputSplitNum assertions below pin that a NaN-free file is STILL pruned, so a
// future "just stop pushing float ranges" regression is caught as well as a re-introduced row loss.
//
// Data (docker/thirdparties/docker-compose/iceberg/scripts/create_preinstalled_scripts/iceberg/run32.sql),
// one file per row group of the listing:
//   nan_filter_double  file1 {1.0, NaN}  file2 {NaN}  file3 {8.0}  file4 {1.0}
//   nan_filter_float   file1 {1.0, NaN}  file2 {2.0}
suite("test_iceberg_nan_filter", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_nan_filter"

    sql """set enable_external_table_batch_mode=false"""
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
        sql """switch ${catalog_name}"""
        sql """use test_db"""

        def ids = { String query -> sql(query).collect { (it[0] as Integer) }.sort() }

        // --- rows: a NaN row satisfies > and >=, so it must survive file pruning --------------------------
        // id=2 and id=3 are the NaN rows, id=4 is 8.0.
        assertEquals([2, 3, 4], ids("select id from nan_filter_double where d > 5"))
        assertEquals([2, 3, 4], ids("select id from nan_filter_double where d >= 5"))
        // The Jira's exact shape: a string literal coerced to double, with every row above the bound.
        assertEquals([1, 2, 3, 4, 5], ids("select id from nan_filter_double where d > '0'"))
        assertEquals([1, 2, 3, 4, 5], ids("select id from nan_filter_double where d >= '0'"))
        // NOT(d < 5) is true for NaN in Doris; iceberg's RewriteNot would otherwise turn it back into d >= 5
        // without the is_nan arm.
        assertEquals([2, 3, 4], ids("select id from nan_filter_double where not (d < 5)"))
        assertEquals([2, 3, 4], ids("select id from nan_filter_double where not (d <= 5)"))

        // --- rows: LT/LE are unchanged -- NaN satisfies neither in Doris ----------------------------------
        assertEquals([1, 5], ids("select id from nan_filter_double where d < 5"))
        assertEquals([1, 5], ids("select id from nan_filter_double where d <= 5"))
        // NOT(d > 5) is false for NaN, so the NaN rows stay out and the NaN files may still be pruned.
        assertEquals([1, 5], ids("select id from nan_filter_double where not (d > 5)"))

        // --- rows: NaN literals ---------------------------------------------------------------------------
        // Doris compares NaN = NaN as true, so the two NaN rows match and nothing else does.
        assertEquals([2, 3], ids("select id from nan_filter_double where d = cast('nan' as double)"))
        assertEquals([1, 4, 5], ids("select id from nan_filter_double where d != cast('nan' as double)"))
        assertEquals([1, 2, 3, 5], ids("select id from nan_filter_double where d in (1.0, cast('nan' as double))"))

        // --- rows: FLOAT column behaves like DOUBLE -------------------------------------------------------
        assertEquals([2], ids("select id from nan_filter_float where f > 5"))
        assertEquals([2], ids("select id from nan_filter_float where f >= 5"))
        assertEquals([1, 3], ids("select id from nan_filter_float where f < 5"))

        // --- pruning: the NaN-free file below the bound must still be pruned -------------------------------
        // d > 5 keeps file1 (hidden NaN), file2 (all NaN) and file3 (8.0), and drops file4 ({1.0}, no NaN).
        explain {
            sql("select id from nan_filter_double where d > 5")
            contains "inputSplitNum=3"
        }
        explain {
            sql("select id from nan_filter_double where d >= 5")
            contains "inputSplitNum=3"
        }
        explain {
            sql("select id from nan_filter_double where not (d < 5)")
            contains "inputSplitNum=3"
        }
        // d < 5 keeps only the two files whose lower bound is below 5; the all-NaN file is correctly pruned.
        explain {
            sql("select id from nan_filter_double where d < 5")
            contains "inputSplitNum=2"
        }
        // f > 5 keeps only the file that may hold a NaN.
        explain {
            sql("select id from nan_filter_float where f > 5")
            contains "inputSplitNum=1"
        }

        // --- the pushed predicate itself carries the is_nan arm -------------------------------------------
        explain {
            sql("verbose select id from nan_filter_double where d > 5")
            contains "icebergPredicatePushdown"
            contains "is_nan"
        }
        // LT keeps the plain predicate: no is_nan arm, no lost pruning.
        explain {
            sql("verbose select id from nan_filter_double where d < 5")
            notContains "is_nan"
        }
    } finally {
        sql """switch internal"""
        sql """drop catalog if exists ${catalog_name}"""
    }
}
