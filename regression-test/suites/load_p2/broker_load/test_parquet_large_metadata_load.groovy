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

suite("test_parquet_large_metadata_load_p2", "p2") {
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def tables = ["parquet_large_metadata_100mb" // metadata size more than 100MB
    ]
    def paths = ["s3://${getS3BucketName()}/regression/load/metadata/parquet_large_metadata_100mb.parquet"
    ]
    String ak = getS3AK()
    String sk = getS3SK()
    String enabled = context.config.otherConfigs.get("enableBrokerLoad")

    def expect_tvf_result = """[[2, 8], [2, 8], [2, 8], [2, 8], [2, 8]]"""
    String[][] tvf_result = sql """select `1`,`2` from s3(
                                     "uri" = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/load/metadata/parquet_large_metadata_100mb.parquet",
                                     "s3.access_key" = "$ak",
                                     "s3.secret_key" = "$sk",
                                     "s3.region" = "${s3Region}",
                                     "provider" = "${getS3Provider()}",
                                     "format" = "parquet"
                                   ) order by `1`,`2` limit 5;
                                """
    assertTrue("$tvf_result" == "$expect_tvf_result")
            
    def do_load_job = { uuid, path, table ->
        sql """
            LOAD LABEL $uuid (
                APPEND
                DATA INFILE("$path")
                INTO TABLE $table
                FORMAT AS "PARQUET"
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}",
                "provider" = "${getS3Provider()}"
            )
            PROPERTIES
            (
                "strict_mode"="true"
            ); 
            """
        logger.info("Submit load with lable: $uuid, table: $table, path: $path")
    }
    
    def etl_info = ["unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=45000"]
    def task_info = ["cluster:${s3Endpoint}; timeout(s):14400; max_filter_ratio:0.0"]
    def error_msg = [""]
    // test unified load
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def uuids = []
        try {
            def i = 0

            for (String table in tables) {
                sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text

                def uuid = UUID.randomUUID().toString().replace("-", "0")
                uuids.add(uuid)
                do_load_job.call(uuid, paths[i], table)
                i++
            }

            i = 0
            for (String label in uuids) {
                def max_try_milli_secs = 600000
                while (max_try_milli_secs > 0) {
                    String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
                    if (result[0][2].equals("FINISHED")) {
                        logger.info("Load FINISHED " + label)
                        assertTrue(result[0][6].contains(task_info[i]))
                        assertTrue(etl_info[i] == result[0][5], "expected: " + etl_info[i] + ", actual: " + result[0][5] + ", label: $label")
                        break;
                    }
                    if (result[0][2].equals("CANCELLED")) {
                        assertTrue(result[0][6].contains(task_info[i]))
                        assertTrue(result[0][7].contains(error_msg[i]))
                        break;
                    }
                    Thread.sleep(1000)
                    max_try_milli_secs -= 1000
                    if(max_try_milli_secs <= 0) {
                        assertTrue(1 == 2, "load Timeout: $label")
                    }
                }
                i++
            }
                        
            def expect_result = """[[45000]]"""

            for (String table in tables) {
                if (table.matches("parquet_large_metadata_100mb")) {
                    String[][] actual_result = sql """select count(*) from parquet_large_metadata_100mb;"""
                    assertTrue("$actual_result" == "$expect_result")
                }
            }

        } finally {
            for (String table in tables) {
                sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
            }
        }
    }
}
