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

import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_outfile_parquet_schema", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = getS3BucketName();

    def export_table_name = "outfile_parquet_schema_test"
    def outFilePath = "${bucket}/outfile/outfile_parquet_schema_test/exp_"

    // create table to export data
    sql """ DROP TABLE IF EXISTS ${export_table_name} """
    sql """
        CREATE TABLE IF NOT EXISTS ${export_table_name} (
        `id` int(11) NULL,
        `name` string NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """
    // insert data
    sql """insert into ${export_table_name} values(1, "abc");"""

    def check_outfile_data = { outfile_url, outfile_format ->
        order_qt_select_tvf """ SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "${outfile_format}",
                            "region" = "${region}"
                        );
                        """
    }

    def check_outfile_column_name = { outfile_url, outfile_format ->
        order_qt_desc_s3 """ Desc function S3 (
                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                    "ACCESS_KEY"= "${ak}",
                    "SECRET_KEY" = "${sk}",
                    "format" = "${outfile_format}",
                    "region" = "${region}"
                );
                """
    }

    def test_q1 = { outfile_format ->
        order_qt_select_base1 """ select * from ${export_table_name} """

        // select ... into outfile ...
        def res = sql """
            SELECT id, name FROM ${export_table_name}
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ${outfile_format}
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "schema" = "required,byte_array,id;required,byte_array,name"
            );
        """
        def outfile_url = res[0][3]
        
        check_outfile_data(outfile_url, outfile_format)
        check_outfile_column_name(outfile_url, outfile_format)
    }

    // test parquet format
    test_q1("parquet")
}
