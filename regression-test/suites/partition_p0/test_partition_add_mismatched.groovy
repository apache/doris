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

suite("test_partition_add_mismatched") {
    sql "drop table if exists test_partition_add_mismatched_list_tbl"
    sql """
        CREATE TABLE IF NOT EXISTS test_partition_add_mismatched_list_tbl (
            k1 int NOT NULL, 
            k2 bigint NOT NULL
        ) 
        PARTITION BY LIST(k1,k2) () 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """

    sql "drop table if exists test_partition_add_mismatched_range_tbl"
    sql """
        CREATE TABLE IF NOT EXISTS test_partition_add_mismatched_range_tbl (
            k1 int NOT NULL, 
            k2 bigint NOT NULL
        ) 
        PARTITION BY RANGE(k1,k2) () 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """

	test{
        sql """ alter table test_partition_add_mismatched_list_tbl add partition p0 values [("0"), ("2")) """
        exception "List partition expected 'VALUES [IN or ((\"xxx\", \"xxx\"), ...)]'"
	}

	test{
        sql """ alter table test_partition_add_mismatched_range_tbl add partition p0 values in (("0", "2")) """
        exception "Range partition expected 'VALUES [LESS THAN or [(\"xxx\" ,...), (\"xxx\", ...))]'"
	}
}
