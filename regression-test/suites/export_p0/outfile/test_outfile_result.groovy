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

suite("test_outfile_result", "p0") {
    def export_table_name = "test_outfile_result"

    sql """ DROP TABLE IF EXISTS ${export_table_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${export_table_name} (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `Name` STRING COMMENT "用户年龄",
        `Age` int(11) NULL
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 10; i ++) {
        sb.append("""
            (${i}, 'ftw-${i}', ${i + 18}),
        """)
    }
    sb.append("""
            (${i}, NULL, NULL)
        """)
    sql """ INSERT INTO ${export_table_name} VALUES
            ${sb.toString()}
        """
    qt_select_export """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

    def isNumber = {String str ->
        logger.info("str = " + str)
        if (str == null || str.trim().isEmpty()) {
            throw exception("result is null or empty")
        }
        try {
            double num = Double.parseDouble(str);
            if (num < 0) {
                throw exception("result can not be less than 0")
            }
        } catch (NumberFormatException e) {
            throw exception("NumberFormatException: " + e.getMessage())
        }
        return true
    }


    // 1. test s3
    try {
        String ak = getS3AK()
        String sk = getS3SK()
        String s3_endpoint = getS3Endpoint()
        String region = getS3Region()
        String bucket = context.config.otherConfigs.get("s3BucketName");

        // http schema
        def outFilePath = "${bucket}/outfile_different_s3/exp_"
        List<List<Object>> outfile_res = sql """ SELECT * FROM ${export_table_name} t ORDER BY user_id
                                                INTO OUTFILE "s3://${outFilePath}"
                                                FORMAT AS parquet
                                                PROPERTIES (
                                                    "s3.endpoint" = "${s3_endpoint}",
                                                    "s3.region" = "${region}",
                                                    "s3.secret_key"="${sk}",
                                                    "s3.access_key" = "${ak}"
                                                );
                                            """

        assertEquals(6, outfile_res[0].size())
        assertEquals(true, isNumber(outfile_res[0][4]))
        assertEquals(true, isNumber(outfile_res[0][5]))
    } finally {
    }
}
