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

suite("test_tvf_error_column", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    String path = "regression/load_p0/tvf"

    def tableName = "test_tvf_error_column"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int NULL,
            `k2` datetime(3) NOT NULL,
            `v1` varchar(5)  REPLACE NOT NULL,
            `v2` varchar(5)  REPLACE NULL,
            `v3` varchar(5)  REPLACE NULL,
            `v4` varchar(5)  REPLACE NULL
        ) ENGINE=OLAP
        aggregate KEY(`k1`,`k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // Step1: date length  overflow
    test {
        sql """       
                INSERT INTO ${tableName}
                        SELECT * FROM S3 (
                                "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_length_limit.csv",
                                "ACCESS_KEY"= "${ak}",
                                "SECRET_KEY" = "${sk}",
                                "format" = "csv",
                                "column_separator" = ",",
                                "enclose" = "\\"",
                                "region" = "${region}"
                                );
        """
        exception "Insert has filtered data in strict mode"
    }


    // Step2: column with wrong type 
    test {
        sql """       
                INSERT INTO ${tableName}
                        SELECT * FROM S3 (
                                "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_column_with_wrong_type.csv",
                                "ACCESS_KEY"= "${ak}",
                                "SECRET_KEY" = "${sk}",
                                "format" = "csv",
                                "column_separator" = ",",
                                "enclose" = "\\"",
                                "region" = "${region}"
                                );
        """
        exception "parse number fail"
    }

    // Step3: NOT NULL column with NULL data
    /*
    test {
        sql """       
                INSERT INTO ${tableName}
                        SELECT * FROM S3 (
                                "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_not_column_with_null_data.csv",
                                "ACCESS_KEY"= "${ak}",
                                "SECRET_KEY" = "${sk}",
                                "format" = "csv",
                                "column_separator" = ",",
                                "region" = "${region}"
                                );
        """
        exception "Insert has filtered data in strict mode"
    }

    // Step4: NOT NULL column with NULL data
    test {
        sql """       
                INSERT INTO ${tableName}
                        SELECT * FROM S3 (
                                "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_not_null_column_with_empty_string_data.csv",
                                "ACCESS_KEY"= "${ak}",
                                "SECRET_KEY" = "${sk}",
                                "format" = "csv",
                                "column_separator" = ",",
                                "region" = "${region}"
                                );
        """
        exception "Insert has filtered data in strict mode"
    }
    */

    // Step5: error compression type
    test {
        sql """       
                INSERT INTO ${tableName}
                        SELECT * FROM S3 (
                                "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_varchar_quotatio.csv",
                                "ACCESS_KEY"= "${ak}",
                                "SECRET_KEY" = "${sk}",
                                "format" = "parquet",
                                "column_separator" = ",",
                                "region" = "${region}"
                                );
        """
        exception "Invalid magic number in parquet file"
    }


    // Step6: error compression type
    test {
        sql """       
                INSERT INTO ${tableName}
                        SELECT * FROM S3 (
                                "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_varchar_quotatio.csv.zip",
                                "ACCESS_KEY"= "${ak}",
                                "SECRET_KEY" = "${sk}",
                                "format" = "csv",
                                "compress" = "gz",
                                "column_separator" = ",",
                                "region" = "${region}"
                                );
        """
        exception "Only support csv data in utf8 codec"
    }

    // Step7: don't support lz4 at this moment
    // test {
    //     sql """       
    //             INSERT INTO ${tableName}
    //                     SELECT * FROM S3 (
    //                             "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_symbol.csv.gz",
    //                             "ACCESS_KEY"= "${ak}",
    //                             "SECRET_KEY" = "${sk}",
    //                             "format" = "csv",
    //                             "compress" = "lz4".
    //                             "column_separator" = ",",
    //                             "region" = "${region}"
    //                             );
    //     """
    //     exception "insert into cols should be corresponding to the query output"
    // }


}
