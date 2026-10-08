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

suite("test_external_scan_consistent_hash_spread", "p0,external") {
    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = getS3BucketName()
    String pathStyle = getS3Provider().equalsIgnoreCase("S3") ? "true" : "false"
    String prefix = "s3://${bucket}/test_external_scan_consistent_hash_spread/${UUID.randomUUID()}/part_"

    sql "drop table if exists test_external_scan_consistent_hash_spread"
    sql """
        create table test_external_scan_consistent_hash_spread (id int, value string)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """
    sql "insert into test_external_scan_consistent_hash_spread values (1, 'alpha'), (2, null), (3, 'gamma')"
    sql """
        select id, value from test_external_scan_consistent_hash_spread order by id
        into outfile "${prefix}single_" format as parquet
        properties (
            "s3.endpoint" = "${endpoint}", "s3.region" = "${region}",
            "s3.access_key" = "${ak}", "s3.secret_key" = "${sk}",
            "s3.path_style_access" = "${pathStyle}"
        )
    """

    def remoteQuery = { pattern ->
        """select id, value from s3(
            "uri" = "${pattern}", "format" = "parquet",
            "s3.endpoint" = "${endpoint}", "s3.region" = "${region}",
            "s3.access_key" = "${ak}", "s3.secret_key" = "${sk}",
            "use_path_style" = "${pathStyle}"
        ) order by id"""
    }
    // Disable result caching so every setting exercises an external scan.
    sql "set enable_sql_cache = false"
    sql "set enable_query_cache = false"
    sql "set enable_file_cache = false"
    sql "set use_consistent_hash_for_external_scan = true"
    for (int candidates : [1, 2, 3, 2147483647]) {
        sql "set external_scan_consistent_hash_spread_num = ${candidates}"
        order_qt_remote remoteQuery("${prefix}single_*.parquet")
    }

    sql "set external_scan_consistent_hash_spread_num = 3"
    sql "set enable_file_cache = true"
    sql "set use_consistent_hash_for_external_scan = false"
    order_qt_cache remoteQuery("${prefix}single_*.parquet")
    sql "set enable_file_cache = false"
    order_qt_round_robin remoteQuery("${prefix}single_*.parquet")

    sql """
        select id, value from test_external_scan_consistent_hash_spread order by id
        into outfile "${prefix}second_" format as parquet
        properties (
            "s3.endpoint" = "${endpoint}", "s3.region" = "${region}",
            "s3.access_key" = "${ak}", "s3.secret_key" = "${sk}",
            "s3.path_style_access" = "${pathStyle}"
        )
    """
    sql "set use_consistent_hash_for_external_scan = true"
    order_qt_multiple_files remoteQuery("${prefix}*.parquet")
    sql "unset variable external_scan_consistent_hash_spread_num"
    order_qt_unset remoteQuery("${prefix}single_*.parquet")
}
