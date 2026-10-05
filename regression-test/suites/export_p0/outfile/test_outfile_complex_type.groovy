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

suite("test_outfile_complex_type", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def export_table_name = "test_outfile_complex_type_table"
    def outFilePath = "${bucket}/outfile/complex_type/exp_"

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
            `price` quantile_state QUANTILE_UNION NOT NULL COMMENT "",
            `hll_t` hll hll_union,
            `device_id` bitmap BITMAP_UNION
        ) ENGINE=OLAP
        AGGREGATE KEY(`dt`, `id`)
        DISTRIBUTED BY HASH(`dt`)
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        INSERT INTO `${export_table_name}` values
        (20220201,0, to_quantile_state(1, 2048), hll_hash(1), to_bitmap(243)),
        (20220201,1, to_quantile_state(-1, 2048), hll_hash(2), bitmap_from_array([1,2,3,4,5,434543])),
        (20220201,2, to_quantile_state(0, 2048), hll_hash(3), to_bitmap(1234566)),
        (20220201,3, to_quantile_state(1, 2048), hll_hash(4), to_bitmap(8888888888888)),
        (20220201,4, to_quantile_state(2, 2048), hll_hash(5), to_bitmap(98392819412234)),
        (20220201,5, to_quantile_state(3, 2048), hll_hash(6), to_bitmap(253234234));
        """

    // parquet file format
    def format = "parquet"
    def outfile_url = outfile_to_S3("${format}")
    qt_select_load_parquet """ SELECT dt, id, hex(price), hex(hll_t) FROM S3 (
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
    qt_select_load_orc """ SELECT dt, id, hex(price), hex(hll_t) FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${format}",
                "region" = "${region}"
            );
            """

    // csv file foramt
    format = "csv"
    outfile_url = outfile_to_S3("${format}")
    qt_select_load_csv """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${format}",
                "region" = "${region}"
            );
            """
}