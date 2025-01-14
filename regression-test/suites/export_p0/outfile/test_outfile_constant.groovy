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

suite("test_outfile_constant", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def export_table_name = "test_outfile_constant"
    def outFilePath = "${bucket}/outfile/constant_type/exp_"

    def outfile_to_S3 = { select_stmt, format ->
        // select ... into outfile ...
        def res = sql """
            ${select_stmt}
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

    def s3_tvf = { outfile_url, format ->
       order_qt_s3_select """ SELECT * FROM S3 (
            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${format}",
            "ACCESS_KEY"= "${ak}",
            "SECRET_KEY" = "${sk}",
            "format" = "${format}",
            "region" = "${region}"
        );
        """ 
    }

    sql """ DROP TABLE IF EXISTS ${export_table_name} """
    sql """
        CREATE TABLE `${export_table_name}` (
            `id` int(11) NULL,
            `Name` string NULL,
            `age` int(11) NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`)
        PROPERTIES (
            "replication_num" = "1"
        );
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
    def insert_res = sql "show last insert;"
    logger.info("insert result: " + insert_res.toString())
    qt_select_export """ SELECT * FROM ${export_table_name} t ORDER BY id; """


    def test_outfile_constant = { format ->
        // select 1
        def outfile_url = outfile_to_S3("Select 1", "${format}")
        s3_tvf("${outfile_url}", "${format}")

        // select 3.1415926
        outfile_url = outfile_to_S3("Select 3.1415926", "${format}")
        s3_tvf("${outfile_url}", "${format}")

        // select e()
        outfile_url = outfile_to_S3("Select e()", "${format}")
        s3_tvf("${outfile_url}", "${format}")

        // select Pi()
        outfile_url = outfile_to_S3("Select Pi()", "${format}")
        s3_tvf("${outfile_url}", "${format}")

        // select 2024-12-09;
        outfile_url = outfile_to_S3("Select 2024-12-09", "${format}")
        s3_tvf("${outfile_url}", "${format}")

        // select "2024-12-09";
        outfile_url = outfile_to_S3("""Select "2024-12-09" """, "${format}")
        s3_tvf("${outfile_url}", "${format}")

        // select null
        outfile_url = outfile_to_S3("Select null", "${format}")
        s3_tvf("${outfile_url}", "${format}")
    }

    test_outfile_constant("parquet");
    test_outfile_constant("orc");
    test_outfile_constant("csv");
    
}