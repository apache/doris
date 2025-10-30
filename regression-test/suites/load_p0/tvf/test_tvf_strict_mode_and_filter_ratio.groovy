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

suite("test_tvf_strict_mode_and_filter_ratio", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    String path = "regression/load_p0/tvf"

    // 1. number overflow
    def csvFile = """test_decimal_overflow.csv"""
    // 1.1 enable_insert_strict=false, load success
    sql """ DROP TABLE IF EXISTS test_insert_select_tvf_strict_mode_and_filter_ratio """
    sql """
    CREATE TABLE test_insert_select_tvf_strict_mode_and_filter_ratio
    (
        k00 DECIMALV3(10,0)
    )
    PROPERTIES ("replication_num" = "1");
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}",
                            "column_separator" = "|"
                            );
                     """
    qt_sql_number_overflow_non_strict "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"

    // 1.2 number overflow, enable_insert_strict=true, insert_max_filter_ratio=1, fail
    sql """
       truncate TABLE test_insert_select_tvf_strict_mode_and_filter_ratio;
"""
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
       sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                               "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                               "ACCESS_KEY"= "${ak}",
                               "SECRET_KEY" = "${sk}",
                               "format" = "csv",
                               "region" = "${region}",
                               "column_separator" = "|"
                               );
                        """
        exception """parse number fail"""
    }
    qt_sql_number_overflow_strict0 "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"

    // 2. not number to number
    csvFile = """test_not_number.csv"""
    // 2.1 not number to number, enable_insert_strict=false, insert_max_filter_ratio=0, success
    sql """ DROP TABLE IF EXISTS test_insert_select_tvf_strict_mode_and_filter_ratio """
    sql """
    CREATE TABLE test_insert_select_tvf_strict_mode_and_filter_ratio
    (
        k00 DECIMALV3(10,0)
    )
    PROPERTIES ("replication_num" = "1");
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}",
                            "column_separator" = "|"
                            );
                     """
    qt_sql_not_number_to_number_non_strict "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"

    // 2.2 not number to number, enable_insert_strict=true, insert_max_filter_ratio=1, fail
    sql """
        truncate TABLE test_insert_select_tvf_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
       sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                               "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                               "ACCESS_KEY"= "${ak}",
                               "SECRET_KEY" = "${sk}",
                               "format" = "csv",
                               "region" = "${region}",
                               "column_separator" = "|"
                               );
                        """
        exception """parse number fail"""
    }
    qt_sql_not_number_to_number_strict "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"

    // 3. null value to not null column
    csvFile = """test_null_number.csv"""
    // 3.1 null value to not null column, enable_insert_strict=false, insert_max_filter_ratio=0.2, fail
    sql """
        DROP TABLE IF EXISTS test_insert_select_tvf_strict_mode_and_filter_ratio;
    """
    sql """
    CREATE TABLE test_insert_select_tvf_strict_mode_and_filter_ratio
    (
        k00 DECIMALV3(10,0) NOT NULL
    )
    PROPERTIES ("replication_num" = "1");
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0.2"
    test {
       sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                               "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                               "ACCESS_KEY"= "${ak}",
                               "SECRET_KEY" = "${sk}",
                               "format" = "csv",
                               "region" = "${region}",
                               "column_separator" = "|"
                               );
                        """
        exception """Insert has too many filtered data"""
        exception """url"""
    }
    qt_sql_not_null_to_null_non_strict0 "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"

    // 3.2 null value to not null column, enable_insert_strict=false, insert_max_filter_ratio=0.3, success
    sql """
        truncate TABLE test_insert_select_tvf_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0.3"
    sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}",
                            "column_separator" = "|"
                            );
                     """
    qt_sql_not_null_to_null_non_strict1 "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"

    // 3.3 null value to not null column, enable_insert_strict=true, insert_max_filter_ratio=1, fail
    sql """
        truncate TABLE test_insert_select_tvf_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
       sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                               "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                               "ACCESS_KEY"= "${ak}",
                               "SECRET_KEY" = "${sk}",
                               "format" = "csv",
                               "region" = "${region}",
                               "column_separator" = "|"
                               );
                        """
        exception """Insert has filtered data in strict mode"""
    }
    qt_sql_not_null_to_null_strict1 "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"

    // 4. no partition
    // 4.1 no partition, enable_insert_strict=false, insert_max_filter_ratio=0.2, load fail
    csvFile = """test_no_partition.csv"""
    sql """ drop table if exists test_insert_select_tvf_strict_mode_and_filter_ratio """
    sql """
        create table test_insert_select_tvf_strict_mode_and_filter_ratio (
          id int,
          name string
        ) PARTITION BY RANGE(`id`)
          (
              PARTITION `p0` VALUES LESS THAN ("60"),
              PARTITION `p1` VALUES LESS THAN ("80")
          )
        properties (
          'replication_num' = '1'
        );
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0.2"
    qt_sql_no_partition_non_strict0 "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"
    test {
       sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                               "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                               "ACCESS_KEY"= "${ak}",
                               "SECRET_KEY" = "${sk}",
                               "format" = "csv",
                               "region" = "${region}",
                               "column_separator" = "|"
                               );
                        """
        exception """Insert has too many filtered data"""
        exception """url"""
    }
    qt_sql_no_partition_non_strict0 "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"

    // 4.2 no partition, enable_insert_strict=false, insert_max_filter_ratio=0.3, load success
    sql """
        truncate table test_insert_select_tvf_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0.3"
    sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}",
                            "column_separator" = "|"
                            );
                     """
    qt_sql_no_partition_non_strict1 "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"

    // 4.3 no partition, enable_insert_strict=true, insert_max_filter_ratio=1, load fail
    sql """
        truncate table test_insert_select_tvf_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
       sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                               "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                               "ACCESS_KEY"= "${ak}",
                               "SECRET_KEY" = "${sk}",
                               "format" = "csv",
                               "region" = "${region}",
                               "column_separator" = "|"
                               );
                        """
        exception """Encountered unqualified data, stop processing"""
        exception """url"""
    }
    qt_sql_no_partition_strict0 "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"

    // 5. string exceed schema length
    csvFile = """test_no_partition.csv"""
    // 5.1 string exceed schema length, enable_insert_strict=false, insert_max_filter_ratio=0, load success
    sql """
        drop table if exists test_insert_select_tvf_strict_mode_and_filter_ratio;
    """
    sql """
        create table test_insert_select_tvf_strict_mode_and_filter_ratio (
          id int,
          name char(10)
        ) properties ('replication_num' = '1');
    """
    sql "set enable_insert_strict=false"
    sql "set enable_strict_cast=true"
    sql "set insert_max_filter_ratio=0"
    sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}",
                            "column_separator" = "|"
                            );
                     """
    qt_sql_string_exceed_len_non_strict0 "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"

    // 5.2 string exceed schema length, enable_insert_strict=true, insert_max_filter_ratio=1, load fail
     sql """
        truncate table test_insert_select_tvf_strict_mode_and_filter_ratio;
    """
    sql "set enable_insert_strict=true"
    sql "set enable_strict_cast=false"
    sql "set insert_max_filter_ratio=1"
    test {
       sql """ insert into test_insert_select_tvf_strict_mode_and_filter_ratio SELECT * FROM S3 (
                               "uri" = "http://${bucket}.${s3_endpoint}/${path}/${csvFile}",
                               "ACCESS_KEY"= "${ak}",
                               "SECRET_KEY" = "${sk}",
                               "format" = "csv",
                               "region" = "${region}",
                               "column_separator" = "|"
                               );
                        """

        exception """Encountered unqualified data, stop processing"""
        exception """url"""
    }
    qt_sql_string_exceed_len_strict1 "select * from test_insert_select_tvf_strict_mode_and_filter_ratio order by 1"
}
