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

suite("test_outfile_native", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def tableName = "outfile_native_test"
    def outFilePath = "${bucket}/outfile/native/exp_"

    // Export helper: write to S3 and return the URL output by FE
    def outfile_to_s3 = {
        def res = sql """
            SELECT * FROM ${tableName} t ORDER BY id
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS native
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """
        return res[0][3]
    }

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `id` INT NOT NULL,
            `c_date` DATE NOT NULL,
            `c_dt` DATETIME NOT NULL,
            `c_str` VARCHAR(20),
            `c_int` INT,
            `c_tinyint` TINYINT,
            `c_bool` boolean,
            `c_double` double
        )
        DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """

        // Insert 10 rows of test data (the last row is all NULL)
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10000; i ++) {
            sb.append("""
                (${i}, '2024-01-01', '2024-01-01 00:00:00', 's${i}', ${i}, ${i % 128}, true, ${i}.${i}),
            """)
        }
        sb.append("""
                (${i}, '2024-01-01', '2024-01-01 00:00:00', NULL, NULL, NULL, NULL, NULL)
            """)
        sql """ INSERT INTO ${tableName} VALUES ${sb.toString()} """

        // baseline: local table query result
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY id limit 10; """

        // 1) 导出为 Native 文件到 S3
        def outfileUrl = outfile_to_s3()

        // 2) 从 S3 使用 S3 TVF (format=native) 查询回数据，并与 baseline 对比
        // outfileUrl 形如：s3://bucket/outfile/native/exp_xxx_* ，需要去掉 "s3://bucket" 前缀和末尾的 '*'
        qt_select_s3_native """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${
                        outfileUrl.substring(5 + bucket.length(), outfileUrl.length() - 1)
                    }0.native",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "native",
                "region" = "${region}"
            ) order by id limit 10;
            """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}