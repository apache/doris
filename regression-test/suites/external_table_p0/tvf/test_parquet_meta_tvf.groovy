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
            "uri" = "${basePath}/meta.parquet",
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
            "uri" = "${basePath}/meta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}"
        );
    """

    // parquet_schema
    order_qt_parquet_schema """
        select * from parquet_meta(
            "uri" = "${basePath}/meta.parquet",
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
            "uri" = "${basePath}/empty.parquet",
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
            "uri" = "${basePath}/kvmeta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}",
            "mode" = "parquet_kv_metadata"
        );
    """

    // file metadata
    order_qt_parquet_file_metadata """
        select * from parquet_meta(
            "uri" = "${basePath}/meta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}",
            "mode" = "parquet_file_metadata"
        );
    """

    // file metadata (S3 glob)
    order_qt_parquet_file_metadata_s3_glob """
        select file_name from parquet_meta(
            "uri" = "${basePath}/*meta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}",
            "mode" = "parquet_file_metadata"
        );
    """

    // bloom probe
    order_qt_parquet_bloom_probe """
        select * from parquet_meta(
            "uri" = "${basePath}/bloommeta.parquet",
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
            "uri" = "${basePath}/meta.parquet",
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
            "uri" = "${basePath}/meta.parquet",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "endpoint" = "${endpoint}",
            "region" = "${region}",
            "mode" = "parquet_metadata"
        );
    """

    // parquet_metadata (HDFS, hive3): reuse group0 parquet files in test_hdfs_parquet_group0.groovy
    String hdfs_port = context.config.otherConfigs.get("hive3HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group0/delta_length_byte_array.parquet"
            order_qt_parquet_schema_hdfs """
                select count(*) from parquet_meta(
                    "uri" = "${uri}",
                    "hadoop.username" = "${hdfsUserName}",
                    "mode" = "parquet_schema"
                );
            """

            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group0/datapage_v1-snappy-compressed-checksum.parquet"
            order_qt_parquet_file_metadata_hdfs """
                select count(*) from parquet_meta(
                    "uri" = "${uri}",
                    "hadoop.username" = "${hdfsUserName}",
                    "mode" = "parquet_file_metadata"
                );
            """

            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group0/column_chunk_key_value_metadata.parquet"
            order_qt_parquet_kv_metadata_hdfs """
                select count(*) from parquet_meta(
                    "uri" = "${uri}",
                    "hadoop.username" = "${hdfsUserName}",
                    "mode" = "parquet_kv_metadata"
                );
            """
        } finally {
        }
    }

    // local parquet_meta: scp files to every BE, then query by local file_path
    def dataFilePath = context.config.dataPath + "/external_table_p0/tvf/"
    def outFilePath="/"
    mkdirRemotePathOnAllBE("root", outFilePath)

    def localMetaFile = "${dataFilePath}/meta.parquet"
    def localEmptyFile = "${dataFilePath}/empty.parquet"
    def localKvMetaFile = "${dataFilePath}/kvmeta.parquet"
    def localBloomMetaFile = "${dataFilePath}/bloommeta.parquet"
    def localCompFile = "${dataFilePath}/comp.parquet"
    def localCompArrFile = "${dataFilePath}/comp_arr.parquet"
    def localTFile = "${dataFilePath}/t.parquet"

    for (List<Object> backend : backends) {
        def beHost = backend[1]
        scpFiles("root", beHost, localMetaFile, outFilePath, false)
        scpFiles("root", beHost, localEmptyFile, outFilePath, false)
        scpFiles("root", beHost, localKvMetaFile, outFilePath, false)
        scpFiles("root", beHost, localBloomMetaFile, outFilePath, false)
        scpFiles("root", beHost, localCompFile, outFilePath, false)
        scpFiles("root", beHost, localCompArrFile, outFilePath, false)
        scpFiles("root", beHost, localTFile, outFilePath, false)
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

    order_qt_parquet_file_metadata_local """
        select * from parquet_meta(
            "file_path" = "${outFilePath}/meta.parquet",
            "mode" = "parquet_file_metadata"
        );
    """

    // file metadata (local glob)
    order_qt_parquet_file_metadata_local_glob """
        select count(*) from parquet_meta(
            "file_path" = "${outFilePath}/*meta.parquet",
            "mode" = "parquet_file_metadata"
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

    // complex types: schema validation
    order_qt_parquet_schema_local_comp """
        select
            sum(if(name = 'm1', 1, 0)) as m1_nodes,
            sum(if(name = 'm2', 1, 0)) as m2_nodes,
            max(case
                when logical_type in ('MAP', 'LIST') or converted_type in ('MAP', 'LIST') then 1
                else 0
            end) as has_complex
        from parquet_meta(
            "file_path" = "${outFilePath}/comp.parquet",
            "mode" = "parquet_schema"
        );
    """

    order_qt_parquet_schema_local_comp_arr """
        select
            sum(if(name = 'aa', 1, 0)) as aa_nodes,
            sum(if(name = 'am', 1, 0)) as am_nodes,
            max(case
                when logical_type in ('MAP', 'LIST') or converted_type in ('MAP', 'LIST') then 1
                else 0
            end) as has_complex
        from parquet_meta(
            "file_path" = "${outFilePath}/comp_arr.parquet",
            "mode" = "parquet_schema"
        );
    """

    order_qt_parquet_schema_local_t """
        select
            sum(if(name = 'arr_arr', 1, 0)) as arr_arr_nodes,
            sum(if(name = 'arr_map', 1, 0)) as arr_map_nodes,
            sum(if(name = 'arr_struct', 1, 0)) as arr_struct_nodes,
            sum(if(name = 'map_map', 1, 0)) as map_map_nodes,
            sum(if(name = 'map_arr', 1, 0)) as map_arr_nodes,
            sum(if(name = 'map_struct', 1, 0)) as map_struct_nodes,
            sum(if(name = 'struct_arr_map', 1, 0)) as struct_arr_map_nodes,
            max(case
                when logical_type in ('MAP', 'LIST') or converted_type in ('MAP', 'LIST') then 1
                else 0
            end) as has_complex
        from parquet_meta(
            "file_path" = "${outFilePath}/t.parquet",
            "mode" = "parquet_schema"
        );
    """

    // complex types: metadata/kv/file modes
    order_qt_parquet_metadata_local_comp """
        select row_group_id, column_id, path_in_schema, type, num_values
        from parquet_meta(
            "file_path" = "${outFilePath}/comp.parquet",
            "mode" = "parquet_metadata"
        )
        order by row_group_id, column_id;
    """

    order_qt_parquet_kv_metadata_local_comp """
        select * from parquet_meta(
            "file_path" = "${outFilePath}/comp.parquet",
            "mode" = "parquet_kv_metadata"
        );
    """

    order_qt_parquet_file_metadata_local_comp """
        select file_name, created_by, num_rows, num_row_groups
        from parquet_meta(
            "file_path" = "${outFilePath}/comp.parquet",
            "mode" = "parquet_file_metadata"
        );
    """

    order_qt_parquet_metadata_local_comp_arr """
        select row_group_id, column_id, path_in_schema, type, num_values
        from parquet_meta(
            "file_path" = "${outFilePath}/comp_arr.parquet",
            "mode" = "parquet_metadata"
        )
        order by row_group_id, column_id;
    """

    order_qt_parquet_kv_metadata_local_comp_arr """
        select * from parquet_meta(
            "file_path" = "${outFilePath}/comp_arr.parquet",
            "mode" = "parquet_kv_metadata"
        );
    """

    order_qt_parquet_file_metadata_local_comp_arr """
        select file_name, created_by, num_rows, num_row_groups
        from parquet_meta(
            "file_path" = "${outFilePath}/comp_arr.parquet",
            "mode" = "parquet_file_metadata"
        );
    """

    order_qt_parquet_metadata_local_t """
        select row_group_id, column_id, path_in_schema, type, num_values
        from parquet_meta(
            "file_path" = "${outFilePath}/t.parquet",
            "mode" = "parquet_metadata"
        )
        order by row_group_id, column_id;
    """

    order_qt_parquet_kv_metadata_local_t """
        select * from parquet_meta(
            "file_path" = "${outFilePath}/t.parquet",
            "mode" = "parquet_kv_metadata"
        );
    """

    order_qt_parquet_file_metadata_local_t """
        select file_name, created_by, num_rows, num_row_groups
        from parquet_meta(
            "file_path" = "${outFilePath}/t.parquet",
            "mode" = "parquet_file_metadata"
        );
    """

    // test exception
    test {
        sql """ select * from parquet_meta(); """
        exception "Property 'uri' or 'file_path' is required for parquet_meta"
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
        exception "Property 'uri' or 'file_path' is required for parquet_meta"
    }

    test {
        sql """
            select * from parquet_meta(
                "uri" = " ",
                "mode" = "parquet_metadata"
            );
        """
        exception "Property 'uri' or 'file_path' must contain at least one location"
    }

    test {
        sql """
            select * from parquet_meta(
                "uri" = "meta.parquet",
                "mode" = "parquet_metadata"
            );
        """
        exception "Property 'uri' must contain a scheme for parquet_meta"
    }

    test {
        sql """
            select * from parquet_meta(
                "file_path" = "s3://bucket/path.parquet",
                "mode" = "parquet_metadata"
            );
        """
        exception "Property 'file_path' must not contain a scheme for parquet_meta"
    }

    test {
        sql """
            select * from parquet_meta(
                "file_path" = "${outFilePath}/meta.parquet",
                "mode" = "parquet_unknown"
            );
        """
        exception "Unsupported mode 'parquet_unknown' for parquet_meta"
    }

    test {
        sql """
            select * from parquet_meta(
                "file_path" = "${outFilePath}/meta.parquet",
                "mode" = "parquet_bloom_probe",
                "value" = 1
            );
        """
        exception "Missing 'column' or 'value' for mode parquet_bloom_probe"
    }

    test {
        sql """
            select * from parquet_meta(
                "file_path" = "${outFilePath}/meta.parquet",
                "mode" = "parquet_bloom_probe",
                "column" = "col"
            );
        """
        exception "Missing 'column' or 'value' for mode parquet_bloom_probe"
    }

    test {
        sql """
            select * from parquet_meta(
                "file_path" = "${outFilePath}/__parquet_meta_tvf_no_match_*.parquet",
                "mode" = "parquet_metadata"
            );
        """
        exception "failed to glob"
    }
}
