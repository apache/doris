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

suite("test_array_cross_product_function") {
    // normal test cases
    qt_sql "SELECT cross_product([1, 2, 3], [2, 3, 4])"
    qt_sql "SELECT cross_product([1, 2, 3], [0, 0, 0])"
    qt_sql "SELECT cross_product([0, 0, 0], [1, 2, 3])"
    qt_sql "SELECT cross_product([1, 0, 0], [0, 1, 0])"
    qt_sql "SELECT cross_product([0, 1, 0], [1, 0, 0])"
    qt_sql "SELECT cross_product(NULL, [1, 2, 3])"
    qt_sql "SELECT cross_product([1, 2, 3], NULL)"

    def tableName = "array_cross_product_test"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            vec1 ARRAY<DOUBLE>,
            vec2 ARRAY<DOUBLE>
        )
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1"
    )
    """
    sql """
        INSERT INTO ${tableName} values
        (1, [1, 2, 3], [2, 3, 4]),
        (2, [1, 2, 3], [0, 0, 0]),
        (3, [1, 0, 0], [0, 1, 0]),
        (4, [0, 1, 0], [1, 0, 0]),
        (5, NULL, [1, 0, 0])
    """
    qt_sql "SELECT id, CROSS_PRODUCT(vec1, vec2) from ${tableName} ORDER BY id"
    sql "DROP TABLE IF EXISTS ${tableName}"

    // abnormal test cases
    test {
        sql "SELECT cross_product([1, NULL, 3], [1, 2, 3])"
        exception "First argument for function cross_product cannot have null elements"
    }
    test {
        sql "SELECT cross_product([1, 2, 3], [NULL, 2, 3])"
        exception "Second argument for function cross_product cannot have null elements"
    }
    test {
        sql "SELECT cross_product([1, 2, 3], [1, 2])"
        exception "function cross_product requires arrays of size 3"
    }
    test {
        sql "SELECT cross_product([1, 2], [3, 4])"
        exception "function cross_product requires arrays of size 3"
    }
    test {
        sql "SELECT cross_product([1, 2, 3, 4], [1, 2, 3, 4])"
        exception "function cross_product requires arrays of size 3"
    }
}
