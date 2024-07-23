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

suite("test_outfile_file_suffix", "p0") {
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
                `name` varchar(128) NOT NULL COMMENT ""
                )
            DISTRIBUTED BY HASH(name) PROPERTIES("replication_num" = "1");
        """
        sql """ INSERT INTO ${table_name} values('zhangsan');"""
    }

    def table_name = "test_outfile_file_suffix"
    create_table(table_name)

    def outFilePath = """s3://${bucket}/outfile_"""
    def csv_suffix_result = { file_suffix, file_format ->
        result = sql """
                select * from ${table_name}
                into outfile "${outFilePath}"
                FORMAT AS ${file_format}
                PROPERTIES(
                    "s3.endpoint" = "${s3_endpoint}",
                    "s3.region" = "${region}",
                    "s3.secret_key"="${sk}",
                    "s3.access_key" = "${ak}",
                    "file_suffix" = "${file_suffix}"
                );
            """
        return result[0][3]
    }

    def file_suffix = "txt";
    def file_format = "csv";
    def outfile_url = csv_suffix_result(file_suffix, file_format);
    print("http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${file_suffix}")
    qt_select """ select * from s3(
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${file_suffix}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${file_format}",
                "provider" = "${getS3Provider()}",
                "region" = "${region}"
            );
            """
}
