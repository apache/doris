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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("load") {
    def tables=["test_join", "test_bucket_shuffle_join"]

    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    sql """ insert into test_join values(1),(2),(3),(4),(5) """
    sql """ ALTER TABLE test_bucket_shuffle_join ADD PARTITION p202112 
            VALUES LESS THAN ("2022-01-01 00:00:00") DISTRIBUTED BY HASH(id) BUCKETS 2;"""
    sql """ insert into test_bucket_shuffle_join values(1, "2021-12-01 00:00:00"),
        (2, "2021-12-01 00:00:00"), (3, "2021-12-01 00:00:00")"""
}
