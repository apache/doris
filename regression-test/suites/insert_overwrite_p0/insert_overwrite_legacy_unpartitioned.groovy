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

suite("insert_overwrite_legacy_unpartitioned") {
    sql "drop table if exists test_no_partition_insert"
    sql """
        create table test_no_partition_insert (a int, b int)
        duplicate key(a)
        distributed by hash(a) buckets 32
        properties(
            "replication_allocation"="tag.location.default: 1"
        );
    """

    sql """insert overwrite table test_no_partition_insert
        select 1,2 from test_no_partition_insert minus select 1,2 from test_no_partition_insert;"""
}