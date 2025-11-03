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

suite("decimal_precision") {

    sql """
        CREATE TABLE IF NOT EXISTS test_null_literal_compute (
            id INT,
            name VARCHAR(50),
            age INT,
            score DECIMAL(5,2),
            create_time DATETIME,
            is_active BOOLEAN
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO test_null_literal_compute VALUES 
        (1, 'Alice', 25, 89.5, '2023-01-01 10:00:00', true),
        (2, 'Bob', 30, 76.2, '2023-01-02 11:00:00', true),
        (3, 'Charlie', 22, 92.0, '2023-01-03 12:00:00', false),
        (4, 'David', 28, 85.7, '2023-01-04 13:00:00', true),
        (5, 'Eve', 35, 67.8, '2023-01-05 14:00:00', false);
    """

    sql """
        SELECT name, age, LAG(age, 1) OVER (ORDER BY id) AS prev_age FROM test_null_literal_compute;
    """
}
