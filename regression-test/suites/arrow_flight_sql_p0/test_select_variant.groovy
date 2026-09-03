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

// A VARIANT row holding an empty JSON object carries no payload at all, so the state that makes
// it read back as `{}` lives in the column rather than in the data. The Arrow Flight result
// writer re-materializes the whole block through MutableBlock, one copy more than the MySQL
// writer does, and that copy of a merge-sorted block used to drop the state: the rows of the run
// that was merged first came back as an empty string while the rest came back as `{}`.
// See https://github.com/apache/doris/issues/67367.
suite("test_select_variant", "arrow_flight_sql") {
    def variantV2Function = getFeConfig("enable_variant_v2").toBoolean() ? "parse_to_variant" : ""

    def tableName = "test_select_variant_empty_object"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        create table ${tableName} (k int, v variant null, vn variant not null)
        DUPLICATE key(`k`) distributed by hash (`k`) buckets 1
        properties ("replication_num" = "1", "disable_auto_compaction" = "true");
        """
    // Two loads, so that a sorted read has to merge two runs into one block.
    sql """INSERT INTO ${tableName} VALUES
            (1, ${variantV2Function}('{}'), ${variantV2Function}('{}')),
            (2, ${variantV2Function}('{}'), ${variantV2Function}('{}')),
            (3, ${variantV2Function}('{}'), ${variantV2Function}('{}'))"""
    sql """INSERT INTO ${tableName} VALUES
            (4, ${variantV2Function}('{}'), ${variantV2Function}('{}')),
            (5, ${variantV2Function}('{"a" : 1}'), ${variantV2Function}('{"a" : 1}'))"""

    qt_arrow_flight_sql_variant_unsorted "select k, v, vn from ${tableName} where k = 1"
    qt_arrow_flight_sql_variant_sorted "select k, v, vn from ${tableName} order by k"
    qt_arrow_flight_sql_variant_sorted_desc "select k, v, vn from ${tableName} order by k desc"
    qt_arrow_flight_sql_variant_cast "select k, cast(v as string), cast(vn as string) from ${tableName} order by k"

    // The empty object is a property of the value, not of the result writer that serializes it,
    // so both protocols have to return the same rows.
    for (String query : ["select k, v, vn from ${tableName} order by k",
                         "select k, v, vn from ${tableName} order by k desc",
                         "select k, cast(v as string), cast(vn as string) from ${tableName} order by k"]) {
        assertEquals(jdbc_sql(query).toString(), arrow_flight_sql(query).toString())
    }
}
