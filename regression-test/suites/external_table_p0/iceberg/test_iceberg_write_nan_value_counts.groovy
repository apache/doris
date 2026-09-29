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

// Iceberg keeps NaN out of a column's bounds by spec, so nan_value_counts is the only metadata that can
// prove a float column holds no NaN. Without it, a float range predicate has to keep every file (see
// test_iceberg_float_predicate_pushdown for why). BE now counts NaNs while writing and FE forwards them,
// which is what lets a Doris-written file be pruned again.
//
// The whole chain is asserted through inputSplitNum, end to end: BE counting -> TIcebergColumnStats ->
// IcebergWriterHelper metrics -> manifest -> InclusiveMetricsEvaluator.
suite("test_iceberg_write_nan_value_counts", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_write_nan_value_counts"
    String db_name = catalog_name + "_db"

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

    sql """drop database if exists ${catalog_name}.${db_name} force"""
    sql """create database ${catalog_name}.${db_name}"""
    // Batch mode defers split generation, which makes inputSplitNum in EXPLAIN non-deterministic.
    sql """set enable_external_table_batch_mode=false"""
    sql """use ${catalog_name}.${db_name}"""

    sql """drop table if exists write_nan_finite"""
    sql """create table write_nan_finite (id int, d double)"""
    sql """insert into write_nan_finite values (1, 1.0), (2, 2.0)"""

    sql """drop table if exists write_nan_present"""
    sql """create table write_nan_present (id int, d double)"""
    sql """insert into write_nan_present values (1, 1.0), (2, cast('nan' as double))"""

    // A file BE counted and found NaN-free reports zero, which is what brings pruning back: the bounds are
    // [1.0, 2.0] and iceberg can now rule the file out. Before BE reported NaN counts this was
    // inputSplitNum=1, because an unknown count forced the file to be kept.
    explain {
        sql("select id from write_nan_finite where d > 100")
        contains "inputSplitNum=0"
    }
    explain {
        sql("select id from write_nan_finite where d >= 100")
        contains "inputSplitNum=0"
    }
    // Pruning must stay honest in the other direction: the NaN row satisfies `d > 100` in Doris, so its
    // file carries a positive NaN count and must still be read.
    explain {
        sql("select id from write_nan_present where d > 100")
        contains "inputSplitNum=1"
    }

    // A predicate the bounds themselves satisfy is unaffected by either count.
    explain {
        sql("select id from write_nan_finite where d > 0")
        contains "inputSplitNum=1"
    }

    // ---- The same, written as ORC -----------------------------------------------------------------
    // ORC column statistics carry no NaN count either, and a Doris-written ORC file does report bounds,
    // so without counting it the `OR isNaN` arm would keep every NaN-free ORC file that the bounds alone
    // used to prune. Both writers now feed the same counter.
    sql """drop table if exists write_nan_finite_orc"""
    sql """create table write_nan_finite_orc (id int, d double) properties ("write-format"="orc")"""
    sql """insert into write_nan_finite_orc values (1, 1.0), (2, 2.0)"""

    sql """drop table if exists write_nan_present_orc"""
    sql """create table write_nan_present_orc (id int, d double) properties ("write-format"="orc")"""
    sql """insert into write_nan_present_orc values (1, 1.0), (2, cast('nan' as double))"""

    explain {
        sql("select id from write_nan_finite_orc where d > 100")
        contains "inputSplitNum=0"
    }
    explain {
        sql("select id from write_nan_present_orc where d > 100")
        contains "inputSplitNum=1"
    }

    sql """drop catalog if exists ${catalog_name}"""
}
