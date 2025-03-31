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

suite("test_insert_error_url", "nonConcurrent") {
    def tableName = "test_insert_error_url_tbl"
    sql """drop table if exists ${tableName}"""
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
         L_ORDERKEY    INTEGER NOT NULL,
         L_PARTKEY     INTEGER NOT NULL,
         L_SUPPKEY     INTEGER NOT NULL,
         L_LINENUMBER  INTEGER NOT NULL,
         L_QUANTITY    DECIMAL(15,2) NOT NULL,
         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
         L_DISCOUNT    DECIMAL(15,2) NOT NULL,
         L_TAX         DECIMAL(15,2) NOT NULL,
         L_RETURNFLAG  CHAR(1) NOT NULL,
         L_LINESTATUS  CHAR(1) NOT NULL,
         L_SHIPDATE    DATE NOT NULL,
         L_COMMITDATE  DATE NOT NULL,
         L_RECEIPTDATE DATE NOT NULL,
         L_SHIPINSTRUCT CHAR(25) NOT NULL,
         L_SHIPMODE     CHAR(10) NOT NULL,
         L_COMMENT      VARCHAR(44) NOT NULL
       )
       UNIQUE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
       PARTITION BY RANGE(L_ORDERKEY) (
           PARTITION p2023 VALUES LESS THAN ("5000000") 
       )
       DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3
       PROPERTIES (
         "replication_num" = "1"
       );
     """

    try {
        GetDebugPoint().enableDebugPointForAllBEs("RuntimeState::get_error_log_file_path.block")
        expectExceptionLike({
            sql """
                 insert into ${tableName} select * from S3(
                     "uri" = "http://${getS3BucketName()}.${getS3Endpoint()}/regression/tpch/sf1/lineitem.csv.split01.gz",
                     "s3.access_key"= "${getS3AK()}",
                     "s3.secret_key" = "${getS3SK()}",
                     "s3.region" = "${getS3Region()}",
                     "format" = "csv",
                     "column_separator" = "|"
                    );
            """
        }, "error_log")
    } finally {
       GetDebugPoint().disableDebugPointForAllBEs("RuntimeState::get_error_log_file_path.block")
    }
}