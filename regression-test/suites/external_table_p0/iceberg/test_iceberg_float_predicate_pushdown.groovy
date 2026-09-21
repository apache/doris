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

// Float/double predicate pushdown must not prune iceberg files that hold rows Doris considers matching.
// Doris matches rows with "NaN is greater than everything" and IEEE zero equality (-0.0 == 0.0), while
// iceberg prunes files with Double.compare, where NaN is absent from the bounds entirely and -0.0 sorts
// strictly before +0.0. A pruned file never becomes a split, so the rows are simply missing from the
// result with no error -- which is why every assertion here is on inputSplitNum: it is the exact
// observable the bug moves, and 0 vs 1 is the difference between losing rows and returning them.
//
// Fixtures are created by spark in run32.sql (one data file each, verified metadata):
//   float_prune_nan_only        nan_value_count=1, no bounds          (NaN never reaches the bounds)
//   float_prune_nan_mixed       nan_value_count=1, bounds [1.0, 1.0]  (NaN hides outside the bounds)
//   float_prune_nan_mixed_float same, on a FLOAT column
//   float_prune_negzero         nan_value_count=0, bounds [-0.0, -0.0]
// They must be spark-written: Doris reports no nan_value_counts at all and normalizes zero bounds, so
// Doris-written files do not carry the metadata shape that triggers the pruning.
suite("test_iceberg_float_predicate_pushdown", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_float_predicate_pushdown"

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

    // Batch mode defers split generation, which makes inputSplitNum in EXPLAIN non-deterministic.
    sql """set enable_external_table_batch_mode=false"""
    sql """switch ${catalog_name}"""
    sql """use test_db"""

    // ---- NaN, a file that is entirely NaN -------------------------------------------------------------
    // Before the fix both of these reported inputSplitNum=0 while `select d > 0` projected true.
    explain {
        sql("select id from float_prune_nan_only where d > 0")
        contains "inputSplitNum=1"
    }
    explain {
        sql("select id from float_prune_nan_only where d >= 0")
        contains "inputSplitNum=1"
    }
    // NaN satisfies neither, so the file is still pruned -- pruning must not be disabled wholesale.
    explain {
        sql("select id from float_prune_nan_only where d < 0")
        contains "inputSplitNum=0"
    }
    explain {
        sql("select id from float_prune_nan_only where d = 0")
        contains "inputSplitNum=0"
    }

    // ---- NaN mixed with ordinary values ---------------------------------------------------------------
    // The half a "whole file is NaN" special case would miss: bounds are [1.0, 1.0], so `d > 5` prunes the
    // file on the bounds alone and the NaN row is lost.
    explain {
        sql("select id from float_prune_nan_mixed where d > 5")
        contains "inputSplitNum=1"
    }
    explain {
        sql("select id from float_prune_nan_mixed where d >= 5")
        contains "inputSplitNum=1"
    }
    // NOT over a range: iceberg's RewriteNot lowers NOT(d < 5) back to a bare `d >= 5`, so this needs the
    // NaN arm just as much as the direct comparison does.
    explain {
        sql("select id from float_prune_nan_mixed where not (d < 5)")
        contains "inputSplitNum=1"
    }
    explain {
        sql("select id from float_prune_nan_mixed_float where f > 5")
        contains "inputSplitNum=1"
    }

    // ---- Signed zero ----------------------------------------------------------------------------------
    // The file holds only -0.0. Doris reads -0.0 = 0.0, iceberg orders -0.0 before +0.0, so a bound placed
    // at +0.0 prunes it.
    explain {
        sql("select id from float_prune_negzero where d = 0")
        contains "inputSplitNum=1"
    }
    explain {
        sql("select id from float_prune_negzero where d >= 0")
        contains "inputSplitNum=1"
    }
    // -0.0 is neither > 0 nor < 0, so both still prune.
    explain {
        sql("select id from float_prune_negzero where d > 0")
        contains "inputSplitNum=0"
    }
    explain {
        sql("select id from float_prune_negzero where d < 0")
        contains "inputSplitNum=0"
    }

    sql """drop catalog if exists ${catalog_name}"""
}
