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

suite("test_array_contains_complex_type") {
    // array_contains only supports scalar element types. An array of MAP, STRUCT or ARRAY is
    // rejected when the query is analyzed, instead of failing when BE runs it.
    test {
        sql "SELECT array_contains(array(map('a', 1)), map('a', 1))"
        exception "array_contains does not support complex types"
    }
    test {
        sql "SELECT array_contains(array(named_struct('a', 1)), named_struct('a', 1))"
        exception "array_contains does not support complex types"
    }
    test {
        sql "SELECT array_contains(array(array(1, 2)), array(1, 2))"
        exception "array_contains does not support complex types"
    }

    sql "DROP TABLE IF EXISTS test_array_contains_complex_type"
    sql """
        CREATE TABLE test_array_contains_complex_type (
            id INT,
            m ARRAY<MAP<STRING, INT>>,
            s ARRAY<STRUCT<a:INT>>
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_array_contains_complex_type
        VALUES (1, array(map('a', 1)), array(named_struct('a', 1)))
    """
    test {
        sql "SELECT array_contains(m, map('a', 1)) FROM test_array_contains_complex_type"
        exception "array_contains does not support complex types"
    }
    test {
        sql """
            SELECT id FROM test_array_contains_complex_type
            WHERE array_contains(s, named_struct('a', 1))
        """
        exception "array_contains does not support complex types"
    }
}
