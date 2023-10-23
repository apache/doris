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

suite("test_outfile_exception") {

    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()

    // This is a bucket that doesn't exist
    String bucket = "test-outfile-exception-no-exists"
    
    def tableName = "outfile_exception_test"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
        `date_1` DATEV2 NOT NULL COMMENT "",
        `datetime_1` DATETIMEV2 NOT NULL COMMENT "",
        `datetime_2` DATETIMEV2(3) NOT NULL COMMENT "",
        `datetime_3` DATETIMEV2(6) NOT NULL COMMENT "",
        `city` VARCHAR(20) COMMENT "用户所在城市",
        `age` SMALLINT COMMENT "用户年龄",
        `sex` TINYINT COMMENT "用户性别",
        `bool_col` boolean COMMENT "",
        `int_col` int COMMENT "",
        `bigint_col` bigint COMMENT "",
        `largeint_col` largeint COMMENT "",
        `float_col` float COMMENT "",
        `double_col` double COMMENT "",
        `char_col` CHAR(10) COMMENT "",
        `decimal_col` decimal COMMENT ""
        )
        DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
    """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 10; i ++) {
        sb.append("""
            (${i}, '2017-10-01', '2017-10-01 00:00:00', '2017-10-01', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', 'Beijing', ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}', ${i}),
        """)
    }
    sb.append("""
            (${i}, '2017-10-01', '2017-10-01 00:00:00', '2017-10-01', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """)
    sql """ INSERT INTO ${tableName} VALUES
            ${sb.toString()}
        """
    order_qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

    // check parquet
    test {
        sql """
            select * from ${tableName} t ORDER BY user_id
            into outfile "s3://${bucket}/test_outfile/exp_"
            format as parquet
            properties(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.access_key"= "xx",
                "s3.secret_key" = "yy"
            );
        """

        // check exception
        exception "The specified bucket does not exist"
    }


    // check orc
    test {
        sql """
            select * from ${tableName} t ORDER BY user_id
            into outfile "s3://${bucket}/test_outfile/exp_"
            format as orc
            properties(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.access_key"= "xx",
                "s3.secret_key" = "yy"
            );
        """

        // check exception
        exception "The specified bucket does not exist"
    }


    // check csv
    test {
        sql """
            select * from ${tableName} t ORDER BY user_id
            into outfile "s3://${bucket}/test_outfile/exp_"
            format as csv
            properties(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.access_key"= "xx",
                "s3.secret_key" = "yy"
            );
        """

        // check exception
        exception "The specified bucket does not exist"
    }


    // check csv_with_names
    test {
        sql """
            select * from ${tableName} t ORDER BY user_id
            into outfile "s3://${bucket}/test_outfile/exp_"
            format as csv_with_names
            properties(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.access_key"= "xx",
                "s3.secret_key" = "yy"
            );
        """

        // check exception
        exception "The specified bucket does not exist"
    }


    // check csv_with_names_and_types
    test {
        sql """
            select * from ${tableName} t ORDER BY user_id
            into outfile "s3://${bucket}/test_outfile/exp_"
            format as csv_with_names_and_types
            properties(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.access_key"= "xx",
                "s3.secret_key" = "yy"
            );
        """

        // check exception
        exception "The specified bucket does not exist"
    }
}
