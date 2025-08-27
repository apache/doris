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

suite("test_outfile_csv_compress", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def create_table = {table_name -> 
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                `id` int,
                `name` varchar(128) NOT NULL COMMENT ""
                )
            DISTRIBUTED BY HASH(name) PROPERTIES("replication_num" = "1");
        """
        sql """ INSERT INTO ${table_name} values(1, 'zhangsan');"""
        for (int i = 0; i < 20; i++) {
            sql """ insert into ${table_name} select * from ${table_name};"""
        }
    }

    def table_name = "test_outfile_csv_compress"
    create_table(table_name)

    def outFilePath = """s3://${bucket}/outfile_"""
    def csv_outfile_result = { the_table_name, compression_type ->
        def result = sql """
                select * from ${the_table_name}
                into outfile "${outFilePath}"
                FORMAT AS CSV
                PROPERTIES(
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.secret_key"="${sk}",
                    "s3.access_key" = "${ak}",
                    "compress_type" = "${compression_type}"
                );
            """
        return result[0][3]
    }

    for (String compression_type: ["plain", "gzip", "bzip2", "snappy", "lz4", "zstd"]) {
        def outfile_url = csv_outfile_result(table_name, compression_type);
        print("http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.")
        qt_select """ select c1, c2 from s3(
                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}*",
                    "ACCESS_KEY"= "${ak}",
                    "SECRET_KEY" = "${sk}",
                    "format" = "csv",
                    "provider" = "${getS3Provider()}",
                    "region" = "${region}",
                    "compress_type" = "${compression_type}"
                ) order by c1 limit 10;
                """
        qt_select """ select count(c1), count(c2) from s3(
                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}*",
                    "ACCESS_KEY"= "${ak}",
                    "SECRET_KEY" = "${sk}",
                    "format" = "csv",
                    "provider" = "${getS3Provider()}",
                    "region" = "${region}",
                    "compress_type" = "${compression_type}"
                );
                """
        qt_select """desc function s3(
                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}*",
                    "ACCESS_KEY"= "${ak}",
                    "SECRET_KEY" = "${sk}",
                    "format" = "csv",
                    "provider" = "${getS3Provider()}",
                    "region" = "${region}",
                    "compress_type" = "${compression_type}"
                );
                """
    }

    // test invalid compression_type
    test {
        sql """
                select * from ${table_name}
                into outfile "${outFilePath}"
                FORMAT AS CSV
                PROPERTIES(
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.secret_key"="${sk}",
                    "s3.access_key" = "${ak}",
                    "compress_type" = "unknown"
                );
            """
        exception """please choose one among GZIP, BZIP2, SNAPPY, LZ4, ZSTD or PLAIN"""
    }

    // test empty table
    sql """drop table if exists test_outfile_csv_compress_empty_table"""
    sql """create table test_outfile_csv_compress_empty_table(k1 int) distributed by hash(k1) buckets 1 properties("replication_num" = "1")"""
    def empty_outfile_url = csv_outfile_result("test_outfile_csv_compress_empty_table", "gzip");
    qt_select """desc function s3(
                "uri" = "http://${bucket}.${s3_endpoint}${empty_outfile_url.substring(5 + bucket.length(), empty_outfile_url.length() - 1)}*",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "provider" = "${getS3Provider()}",
                "region" = "${region}",
                "compress_type" = "gzip"
            );
            """
}

