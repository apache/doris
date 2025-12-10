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

suite("test_tvf_error_data", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    String path = "regression/load_p0/tvf"

    def tableName = "test_tvf_error_data"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int NULL,
            `k2` datetime(3) NULL,
            `v1` varchar(50)  NULL,
            `v2` varchar(50)  NULL,
            `v3` varchar(50)  NULL,
            `v4` varchar(50)  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`,`k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // Step1: date with quotatio 
    sql "set enable_insert_strict = false"
    sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_date_quotatio.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "column_separator" = ",",
                            "region" = "${region}"
                            );
       """
    sql "set enable_insert_strict = true"
    qt_select """
            select * from ${tableName}
    """
    sql """ truncate table  ${tableName} """
 
    // Step2: use trim_double_quotes to ignore quotatio
    sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_date_quotatio.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "column_separator" = ",",
                            "trim_double_quotes" = "true",
                            "region" = "${region}"
                            );
       """
    qt_select """
            select * from ${tableName}
    """
    sql """ truncate table  ${tableName} """

    // Step3: string with quotatio init
    sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_varchar_quotatio.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "column_separator" = ",",
                            "region" = "${region}"
                            );
       """
    qt_select """
            select * from ${tableName}
    """
    sql """ truncate table  ${tableName} """

    // Step4: string with double quotatio init
    sql "set enable_insert_strict = false"
    qt_sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_varchar_double_quotatio.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "column_separator" = ",",
                            "region" = "${region}"
                            );
       """
    sql "set enable_insert_strict = true"
    qt_select """
            select * from ${tableName}
    """
    sql """ truncate table  ${tableName} """

    // Step5: string with symbol init
    sql "set enable_insert_strict = false"
    qt_sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_symbol.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "column_separator" = ",",
                            "region" = "${region}"
                            );
       """
    sql "set enable_insert_strict = true"
    qt_select """
            select * from ${tableName}
    """
    sql """ truncate table  ${tableName} """

    // Step6: string with enclose init
    sql "set enable_insert_strict = false"
    qt_sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_enclose.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "column_separator" = ",",
                            "format" = "csv",
                            "enclose" = "'",
                            "region" = "${region}"
                            );
       """
    sql "set enable_insert_strict = true"
    qt_select """
            select * from ${tableName}
    """
    sql """ truncate table  ${tableName} """

    // Step7: string with escape init
    sql "set enable_insert_strict = false"
    qt_sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_enclose.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "column_separator" = ",",
                            "format" = "csv",
                            "enclose" = "'",
                            "escape" = "\",
                            "region" = "${region}"
                            );
       """
    sql "set enable_insert_strict = true"
    qt_select """
            select * from ${tableName}
    """
    sql """ truncate table  ${tableName} """


    // Step8: string with separator init
    sql "set enable_insert_strict = false"
    qt_sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_separator.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "column_separator" = ",",
                            "enclose" = "\\"",
                            "trim_double_quotes" = "true",
                            "region" = "${region}"
                            );
       """
    sql "set enable_insert_strict = true"
    qt_select """
            select * from ${tableName}
    """
    sql """ truncate table  ${tableName} """

    // Step9: string with separator init without enclose
    test {
        sql """       
                INSERT INTO ${tableName}
                        SELECT * FROM S3 (
                                "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_separator.csv",
                                "ACCESS_KEY"= "${ak}",
                                "SECRET_KEY" = "${sk}",
                                "format" = "csv",
                                "column_separator" = ",",
                                "region" = "${region}"
                                );
        """
        exception "insert into cols should be corresponding to the query output"
    }

}
