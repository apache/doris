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

suite("test_non_nullable_iceberg_mtmv", "p0,external,iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    sql "DROP CATALOG IF EXISTS non_nullable_mv_iceberg"
    sql """CREATE CATALOG non_nullable_mv_iceberg PROPERTIES (
        "type" = "iceberg", "iceberg.catalog.type" = "rest", "uri" = "http://${host}:${restPort}",
        "s3.endpoint" = "http://${host}:${minioPort}", "s3.region" = "us-east-1",
        "s3.access_key" = "admin", "s3.secret_key" = "password"
    )"""
    checkNonNullableMvRewrite("non_nullable_mv_iceberg.format_v2.sample_mor_parquet",
            "id", "non_nullable_iceberg_mv")
    String source = "non_nullable_mv_iceberg.format_v2.non_nullable_vector_source"
    sql "DROP TABLE IF EXISTS ${source}"
    sql """CREATE TABLE ${source} (id INT, vec ARRAY<FLOAT>) ENGINE=ICEBERG"""
    sql "INSERT INTO ${source} VALUES (1, [1.0, 0.0]), (2, [2.0, 0.0])"
    checkNonNullableMvRewrite(source, ["id", "vec"], "non_nullable_iceberg_vector_mv")
}
