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

suite("test_parquet_meta_tvf", "p0,external,external_docker,tvf") {
    // use nereids planner
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    List<List<Object>> backends = sql """ show backends """
    assertTrue(backends.size() > 0)

    def ak = getS3AK()
    def sk = getS3SK()
    def endpoint = getS3Endpoint()
    def region = getS3Region()
    def bucket = context.config.otherConfigs.get("s3BucketName")
    def basePath = "s3://${bucket}/regression/datalake/pipeline_data"

    // parquet_metadata (S3)
    // Note: Prefer asserting on stable metadata columns; avoid relying on host-specific/local-only paths.
    order_qt_parquet_metadata_s3 """
        select * from parquet_meta(
            "path" = "${basePath}/meta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}",
            "mode" = "parquet_metadata"
        );
    """

    // default mode: parquet_metadata
    order_qt_parquet_metadata_default_mode """
        select * from parquet_meta(
            "path" = "${basePath}/meta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}"
        );
    """

    // parquet_schema
    order_qt_parquet_schema """
        select * from parquet_meta(
            "path" = "${basePath}/meta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}",
            "mode" = "parquet_schema"
        );
    """

    // empty parquet
    order_qt_parquet_metadata_empty """
        select * from parquet_meta(
            "path" = "${basePath}/empty.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}",
            "mode" = "parquet_metadata"
        );
    """

    // kv metadata
    order_qt_parquet_kv_metadata """
        select * from parquet_meta(
            "path" = "${basePath}/kvmeta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}",
            "mode" = "parquet_kv_metadata"
        );
    """

    // bloom probe
    order_qt_parquet_bloom_probe """
        select * from parquet_meta(
            "path" = "${basePath}/bloommeta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}",
            "mode" = "parquet_bloom_probe",
            "column" = "col",
            "value" = 500
        );
    """

    // bloom probe: column without bloom filter
    order_qt_parquet_bloom_probe_no_bf """
        select * from parquet_meta(
            "path" = "${basePath}/meta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}",
            "mode" = "parquet_bloom_probe",
            "column" = "normal_int",
            "value" = 500
        );
    """

    // mapping select
    order_qt_parquet_mapping """
        select row_group_id, row_group_num_rows, row_group_num_columns, row_group_bytes, column_id,
            file_offset, num_values, path_in_schema, type, stats_min, stats_max, stats_null_count,
            stats_distinct_count, stats_min_value
        from parquet_meta(
            "path" = "${basePath}/meta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}",
            "mode" = "parquet_metadata"
        );
    """

    // local parquet_meta: scp files to every BE, then query by local file_path
    def dataFilePath = context.config.dataPath + "/external_table_p0/tvf/"
    def outFilePath="/"
    mkdirRemotePathOnAllBE("root", outFilePath)

    def localMetaFile = "${dataFilePath}/meta.parquet"
    def localEmptyFile = "${dataFilePath}/empty.parquet"
    def localKvMetaFile = "${dataFilePath}/kvmeta.parquet"
    def localBloomMetaFile = "${dataFilePath}/bloommeta.parquet"

    for (List<Object> backend : backends) {
        def beHost = backend[1]
        scpFiles("root", beHost, localMetaFile, outFilePath, false)
        scpFiles("root", beHost, localEmptyFile, outFilePath, false)
        scpFiles("root", beHost, localKvMetaFile, outFilePath, false)
        scpFiles("root", beHost, localBloomMetaFile, outFilePath, false)
    }

    order_qt_parquet_metadata_local """
        select * from parquet_meta(
            "file_path" = "${outFilePath}/meta.parquet",
            "mode" = "parquet_metadata"
        );
    """

    order_qt_parquet_schema_local """
        select * from parquet_meta(
            "file_path" = "${outFilePath}/meta.parquet",
            "mode" = "parquet_schema"
        );
    """

    order_qt_parquet_metadata_empty_local """
        select * from parquet_meta(
            "file_path" = "${outFilePath}/empty.parquet",
            "mode" = "parquet_metadata"
        );
    """

    order_qt_parquet_kv_metadata_local """
        select * from parquet_meta(
            "file_path" = "${outFilePath}/kvmeta.parquet",
            "mode" = "parquet_kv_metadata"
        );
    """

    order_qt_parquet_bloom_probe_local """
        select * from parquet_meta(
            "file_path" = "${outFilePath}/bloommeta.parquet",
            "mode" = "parquet_bloom_probe",
            "column" = "col",
            "value" = 500
        );
    """

    order_qt_parquet_bloom_probe_no_bf_local """
        select * from parquet_meta(
            "file_path" = "${outFilePath}/meta.parquet",
            "mode" = "parquet_bloom_probe",
            "column" = "normal_int",
            "value" = 500
        );
    """

    order_qt_parquet_metadata_local_mapping """
        select
            row_group_id, row_group_num_rows, row_group_num_columns, row_group_bytes, column_id,
            file_offset, num_values, path_in_schema, type, stats_min, stats_max, stats_null_count,
            stats_distinct_count, stats_min_value
        from parquet_meta(
            "file_path" = "${outFilePath}/meta.parquet",
            "mode" = "parquet_metadata"
        );
    """

    // test exception
    test {
        sql """ select * from parquet_meta(); """
        exception "Property 'uri' or 'path' (or file_path) is required for parquet_meta"
    }

    test {
        sql """
            select * from parquet_meta(
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "endpoint" = "${endpoint}",
                "region" = "${region}",
                "mode" = "parquet_metadata"
            );
        """
        exception "Property 'uri' or 'path' (or file_path) is required for parquet_meta"
    }

    test {
        sql """
            select * from parquet_meta(
                "path" = " ",
                "mode" = "parquet_metadata"
            );
        """
        exception "Property 'uri' or 'path' must contain at least one location"
    }
}
