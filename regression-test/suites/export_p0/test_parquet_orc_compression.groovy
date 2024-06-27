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

suite("test_parquet_orc_compression", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """


    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def table_export_name = "test_parquet_orc_compression"
    def outfile_path_prefix = """${bucket}/export/test_parquet_orc_compression/exp_"""

    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
        CREATE TABLE IF NOT EXISTS ${table_export_name} (
            `user_id` INT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间"
            )
            DISTRIBUTED BY HASH(user_id)
            PROPERTIES("replication_num" = "1");
        """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 11; i ++) {
        sb.append("""
            (${i}, '2017-10-01'),
        """)
    }
    sb.append("""
            (${i}, '2017-10-01')
        """)
    sql """ INSERT INTO ${table_export_name} VALUES
            ${sb.toString()}
        """
    def insert_res = sql "show last insert;"
    logger.info("insert result: " + insert_res.toString())
    order_qt_select_export1 """ SELECT * FROM ${table_export_name} t ORDER BY user_id; """


    def waiting_export = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}";"""
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                def json = parseJson(res[0][11])
                assert json instanceof List
                assertEquals("1", json.fileNumber[0][0])
                log.info("outfile_path: ${json.url[0][0]}")
                return json.url[0][0];
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    // export compression 
    def export_compression = { file_format, compression_type, tvf_read ->
        def uuid = UUID.randomUUID().toString()
        def outFilePath = """${outfile_path_prefix}_${uuid}"""
        def label = "label_${uuid}"
        try {
            // exec export
            sql """
                EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "${file_format}",
                    "compress_type" = "${compression_type}"
                )
                WITH S3(
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.secret_key"="${sk}",
                    "s3.access_key" = "${ak}"
                );
            """

            if (tvf_read) {
                def outfile_url = waiting_export.call(label)
            
                order_qt_select_load1 """ select * from s3(
                                            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${file_format}",
                                            "s3.access_key"= "${ak}",
                                            "s3.secret_key" = "${sk}",
                                            "format" = "${file_format}",
                                            "region" = "${region}"
                                        ) ORDER BY user_id;
                                        """
            }
        } finally {
        }
    }

    // outfile compression
    def outfile_compression = { file_format, compression_type, tvf_read ->
        def uuid = UUID.randomUUID().toString()
        def outFilePath = """${outfile_path_prefix}_${uuid}"""

        def res = sql """
            SELECT * FROM ${table_export_name} t ORDER BY user_id
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ${file_format}
            PROPERTIES (
                "compress_type" = "${compression_type}",
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """

        if (tvf_read) {
            def outfile_url = res[0][3]
            order_qt_select_load1 """ SELECT * FROM S3 (
                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${file_format}",
                    "ACCESS_KEY"= "${ak}",
                    "SECRET_KEY" = "${sk}",
                    "format" = "${file_format}",
                    "region" = "${region}"
                );
                """
        }
    }

    // 1. export
    // 1.1 parquet
    export_compression("parquet", "snappy", true)
    export_compression("parquet", "GZIP", true)
    // parquet-read do not support read BROTLI compression type now
    export_compression("parquet", "BROTLI", false)
    export_compression("parquet", "ZSTD", true)
    export_compression("parquet", "LZ4", true)
    export_compression("parquet", "plain", true)
    // 1.2 orc
    export_compression("orc", "PLAIN", true)
    export_compression("orc", "SNAPPY", true)
    export_compression("orc", "ZLIB", true)
    export_compression("orc", "ZSTD", true)

    // 2. outfile 
    // parquet
    outfile_compression("parquet", "snappy", true)
    outfile_compression("parquet", "GZIP", true)
    // parquet-read do not support read BROTLI compression type now
    outfile_compression("parquet", "BROTLI", false)
    outfile_compression("parquet", "ZSTD", true)
    outfile_compression("parquet", "LZ4", true)
    outfile_compression("parquet", "plain", true)
    // orc
    outfile_compression("orc", "PLAIN", true)
    outfile_compression("orc", "SNAPPY", true)
    outfile_compression("orc", "ZLIB", true)
    outfile_compression("orc", "ZSTD", true)
}
