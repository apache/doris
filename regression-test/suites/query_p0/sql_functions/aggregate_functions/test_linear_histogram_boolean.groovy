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

suite("test_linear_histogram_boolean") {
    sql "DROP TABLE IF EXISTS linear_histogram_boolean"
    sql """
        CREATE TABLE linear_histogram_boolean (
            id INT NOT NULL,
            b BOOLEAN NULL,
            non_null_b BOOLEAN NOT NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO linear_histogram_boolean VALUES
            (1, TRUE, TRUE), (2, FALSE, FALSE), (3, TRUE, TRUE), (4, NULL, FALSE)
    """

    order_qt_nullable """
        SELECT linear_histogram(b, 1.0), linear_histogram(b, 1.0, 0.5)
        FROM linear_histogram_boolean
    """
    order_qt_not_nullable """
        SELECT linear_histogram(non_null_b, 1.0), linear_histogram(non_null_b, 1.0, 0.5)
        FROM linear_histogram_boolean
    """
    order_qt_grouped """
        SELECT id % 2, linear_histogram(b, 1.0), linear_histogram(b, 1.0, 0.5)
        FROM linear_histogram_boolean GROUP BY id % 2
    """
    order_qt_all_null """
        SELECT linear_histogram(b, 1.0), linear_histogram(b, 1.0, 0.5)
        FROM linear_histogram_boolean WHERE b IS NULL
    """
    order_qt_empty """
        SELECT linear_histogram(b, 1.0), linear_histogram(b, 1.0, 0.5)
        FROM linear_histogram_boolean WHERE id < 0
    """

    test {
        sql "SELECT linear_histogram(non_null_b, 0.0) FROM linear_histogram_boolean"
        exception "interval should be larger than 0"
    }
    test {
        sql "SELECT linear_histogram(non_null_b, 1.0, 1.0) FROM linear_histogram_boolean"
        exception "offset should be in [0, interval)"
    }
}
