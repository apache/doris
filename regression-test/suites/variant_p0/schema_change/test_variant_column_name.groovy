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

suite("test_variant_column_name", "variant_type") {
    sql "DROP TABLE IF EXISTS test_variant_column_name"
    sql """CREATE TABLE test_variant_column_name (
        k INT, v VARIANT, `s.x` STRING
    ) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
    PROPERTIES ('replication_num'='1')"""

    [
        "ALTER TABLE test_variant_column_name ADD COLUMN `v.x` VARIANT",
        "ALTER TABLE test_variant_column_name ADD COLUMN (good VARIANT, `v.x` VARIANT)",
        "ALTER TABLE test_variant_column_name MODIFY COLUMN `s.x` VARIANT",
        "ALTER TABLE test_variant_column_name RENAME COLUMN v `v.x`"
    ].each { statement ->
        test {
            sql statement
            exception "`VARIANT` type column named with a `.` character"
        }
    }
    sql "ALTER TABLE test_variant_column_name ADD COLUMN good VARIANT"
    sql "ALTER TABLE test_variant_column_name RENAME COLUMN good renamed"
    sql "ALTER TABLE test_variant_column_name ADD COLUMN `other.x` STRING"
    sql "ALTER TABLE test_variant_column_name RENAME COLUMN `other.x` `other.y`"
}
