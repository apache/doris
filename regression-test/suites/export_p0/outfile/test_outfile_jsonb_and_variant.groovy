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

suite("test_outfile_jsonb_and_variant", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def export_table_name = "test_outfile_jsonb_and_variant_table"
    def outFilePath = "${bucket}/outfile/jsonb_and_variant/exp_"

    def outfile_to_S3 = { format ->
        // select ... into outfile ...
        def res = sql """
            SELECT * FROM ${export_table_name} t
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ${format}
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """

        return res[0][3]
    }

    sql """ DROP TABLE IF EXISTS ${export_table_name} """
    sql """
        CREATE TABLE `${export_table_name}` (
            `dt` int(11) NULL COMMENT "",
            `id` int(11) NULL COMMENT "",
            `json_col` JSON NULL COMMENT "",
            `variant_col` variant NULL COMMENT ""
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`dt`)
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        INSERT INTO `${export_table_name}` values
        (20220201,0, '{"k1": "100"}', '{"k1": "100"}'),
        (20220201,1, '{"k1": "100", "k2": "123"}', '{"k1": "100", "k2": "123"}'),
        (20220201,2, '{"k1": "100", "abc": "567"}', '{"k1": "100", "abc": "567"}'),
        (20220201,3, '{"k1": "100", "k3": 123}', '{"k1": "100", "k3": 123}'),
        (20220201,4, '{"k1": "100", "doris": "nereids"}', '{"k1": "100", "doris": "nereids"}'),
        (20220201,5, '{"k1": "100", "doris": "pipeline"}', '{"k1": "100", "doris": "pipeline"}');
        """

    // parquet file format
    def format = "parquet"
    def outfile_url = outfile_to_S3("${format}")
    qt_select_load_parquet """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${format}",
                "region" = "${region}"
            );
            """

    // orc file foramt
    format = "orc"
    outfile_url = outfile_to_S3("${format}")
    qt_select_load_orc """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${format}",
                "region" = "${region}"
            );
            """

    // orc file foramt
    format = "csv"
    outfile_url = outfile_to_S3("${format}")
    qt_select_load_orc """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${format}",
                "region" = "${region}"
            );
            """
}