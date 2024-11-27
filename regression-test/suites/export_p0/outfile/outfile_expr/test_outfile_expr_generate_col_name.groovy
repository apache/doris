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

suite("test_outfile_expr_generate_col_name", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = getS3BucketName();


    def export_table_name = "outfile_expr_export_test"
    def outFilePath = "${bucket}/outfile/expr_generate_col_name/exp_"


    def create_table = {
        sql """ DROP TABLE IF EXISTS ${export_table_name} """
        sql """
            CREATE TABLE IF NOT EXISTS ${export_table_name} (
            `id` int(11) NULL,
            `name` string NULL,
            `age` int(11) NULL
            )
            PARTITION BY RANGE(id)
            (
                PARTITION less_than_20 VALUES LESS THAN ("20"),
                PARTITION between_20_70 VALUES [("20"),("70")),
                PARTITION more_than_70 VALUES LESS THAN ("151")
            )
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES("replication_num" = "1");
        """
    }

    def outfile_to_S3 = { format ->
        // select ... into outfile ...
        def res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ${outfile_format}
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """

        return res[0][3]
    }

    // create table to export data
    create_table()
        
    // insert data
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 10; i ++) {
        sb.append("""
            (${i}, 'doris-${i}', ${i + 18}),
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
    order_qt_select_base """ SELECT * FROM ${export_table_name} t ORDER BY id; """



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
        order_qt_select_base1 """ select 1>2 from ${export_table_name} """

        // select ... into outfile ...
        def res = sql """
            SELECT 1>2 FROM ${export_table_name}
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ${outfile_format}
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """
        outfile_url = res[0][3]
        
        check_outfile_data(outfile_url, outfile_format)
        check_outfile_column_name(outfile_url, outfile_format)
    }

    def test_q2 = { outfile_format ->
        order_qt_select_base1 """ select max(id) from ${export_table_name} """

        // select ... into outfile ...
        def res = sql """
            SELECT max(id) FROM ${export_table_name}
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ${outfile_format}
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """
        outfile_url = res[0][3]
        
        check_outfile_data(outfile_url, outfile_format)
        check_outfile_column_name(outfile_url, outfile_format)
    }

    def test_q3 = { outfile_format ->
        order_qt_select_base1 """ select id, case id when 1 then 'id = 1' when 2 then 'id = 2' else 'id not exist' end from ${export_table_name} """

        // select ... into outfile ...
        def res = sql """
            SELECT id, case id when 1 then 'id = 1' when 2 then 'id = 2' else 'id not exist' end FROM ${export_table_name}
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ${outfile_format}
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """
        outfile_url = res[0][3]
        
        check_outfile_data(outfile_url, outfile_format)
        check_outfile_column_name(outfile_url, outfile_format)
    }

    def test_q4 = { outfile_format ->
        order_qt_select_base1 """ select 
                                id,
                                1, 
                                'string', 
                                cast (age AS BIGINT),  
                                1 > 2,
                                2 + 3,
                                1 IN (1, 2, 3, 4), 
                                TRUE | FALSE
                            from ${export_table_name}
                        """

        // select ... into outfile ...
        def res = sql """
            select 
                id,
                1, 
                'string', 
                cast (age AS BIGINT),  
                1 > 2,
                2 + 3,
                1 IN (1, 2, 3, 4), 
                TRUE | FALSE
            from ${export_table_name}
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ${outfile_format}
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """
        outfile_url = res[0][3]
        
        check_outfile_data(outfile_url, outfile_format)
        check_outfile_column_name(outfile_url, outfile_format)
    }

    def test_q5 = { outfile_format ->
        order_qt_select_base1 """ select cast('2566' as string), cast('888' as bigint), cast('9999' as largeint)
                            from ${export_table_name}
                        """

        // select ... into outfile ...
        def res = sql """
            select cast('2566' as string), cast('888' as bigint), cast('9999' as largeint)
            from ${export_table_name}
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ${outfile_format}
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """
        outfile_url = res[0][3]
        
        check_outfile_data(outfile_url, outfile_format)
        check_outfile_column_name(outfile_url, outfile_format)
    }

    // test parquet format
    test_q1("parquet")
    test_q2("parquet")
    test_q3("parquet")
    test_q4("parquet")
    test_q5("parquet")

    // test orc format
    test_q1("orc")
    test_q2("orc")
    test_q3("orc")
    test_q4("orc")
    test_q5("orc")
}
