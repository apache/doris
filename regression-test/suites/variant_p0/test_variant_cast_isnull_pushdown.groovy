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

suite("test_variant_cast_isnull_pushdown", "p0") {
    // A Variant cast can turn a non-NULL Variant value into NULL, so `CAST(v AS INT) IS NULL`
    // is not equivalent to `v IS NULL`. The scan node must keep this null test as an expression
    // instead of pushing a null predicate down on the source Variant column.
    sql "DROP TABLE IF EXISTS variant_cast_isnull_pushdown"
    sql """
        CREATE TABLE variant_cast_isnull_pushdown (
            id INT NOT NULL,
            v VARIANT NOT NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO variant_cast_isnull_pushdown VALUES
            (1, '1'),
            (2, '"x"'),
            (3, '[1,2]')
    """

    order_qt_cast_int "SELECT id, CAST(v AS INT) FROM variant_cast_isnull_pushdown ORDER BY id"
    order_qt_is_null "SELECT id FROM variant_cast_isnull_pushdown WHERE CAST(v AS INT) IS NULL ORDER BY id"
    order_qt_is_not_null "SELECT id FROM variant_cast_isnull_pushdown WHERE CAST(v AS INT) IS NOT NULL ORDER BY id"
    order_qt_null_safe "SELECT id FROM variant_cast_isnull_pushdown WHERE CAST(v AS INT) <=> NULL ORDER BY id"
}
