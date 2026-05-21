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

suite("test_variant_count_distinct") {
    sql "DROP TABLE IF EXISTS test_variant_count_distinct_array_subcolumn"

    sql """
        CREATE TABLE test_variant_count_distinct_array_subcolumn (
            id INT,
            v VARIANT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO test_variant_count_distinct_array_subcolumn VALUES
        (1, '{"arr":[1,2,3]}'),
        (2, '{"arr":[4,5]}'),
        (3, '{"arr":[1,2,3]}')
    """

    test {
        sql "SELECT COUNT(DISTINCT v['arr']) FROM test_variant_count_distinct_array_subcolumn"
        exception "COUNT DISTINCT does not support VARIANT argument"
    }
}
