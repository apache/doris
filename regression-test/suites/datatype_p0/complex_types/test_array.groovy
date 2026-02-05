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

suite("test_array") {
    multi_sql """
    set batch_size=3;
    set enable_local_shuffle=false;
    set parallel_pipeline_task_num = 2;
    set enable_new_shuffle_hash_method=true;
    """

    sql "DROP TABLE IF EXISTS `test_array_int_table`"
    multi_sql """
     CREATE TABLE test_array_int_table(
            id INT,
            name VARCHAR(50),
            tags ARRAY<VARCHAR(50)>, 
            price DECIMAL(10,2),
            category_ids ARRAY<INT>,
            cid INT
        ) 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );

        INSERT INTO test_array_int_table  VALUES
        (1, 'Laptop', ['Electronics', 'Office', 'High-End', 'Laptop'], 5999.99, [1, 2, 3], 1),
        (2, 'Mechanical Keyboard', ['Electronics', 'Accessories'], 399.99, [1, 2], 1),
        (3, 'Basketball', ['Sports', 'Outdoor'], 199.99, [1,3], 1),
        (4, 'Badminton Racket', ['Sports', 'Equipment'], 299.99, [3], 3),
        (5, 'Shirt', ['Clothing', 'Office', 'Shirt'], 259.00, [4], 4);

     INSERT INTO test_array_int_table  VALUES
            (1, 'Laptop', ['Electronics', 'Office', 'High-End', 'Laptop'], 5999.99, [1, 2, 3], 2),
            (2, 'Mechanical Keyboard', ['Electronics', 'Accessories'], 399.99, [1, 2], 2),
            (3, 'Basketball', ['Sports', 'Outdoor'], 199.99, [1,3], 3);

     INSERT INTO test_array_int_table  VALUES
            (1, 'Laptop', ['Electronics', 'Office', 'High-End', 'Laptop'], 5999.99, [1, 2, 3], 3);
    """

    qt_window_function_partition_by_array_int """
    select
        name,
        category_ids,
        cid,
        sum(cid) over(partition by category_ids)
    from
        test_array_int_table
    group by
        name, category_ids, cid
    order by
        name, category_ids, cid;
    """
    // test decimal
    sql "DROP TABLE IF EXISTS `test_array_decimal_table`"
    multi_sql """
     CREATE TABLE test_array_decimal_table(
            id INT,
            name VARCHAR(50),
            tags ARRAY<VARCHAR(50)>, 
            price DECIMAL(10,2),
            category_ids ARRAY<decimalv3(10,2)>,
            cid INT
        ) 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );

        INSERT INTO test_array_decimal_table  VALUES
        (1, 'Laptop', ['Electronics', 'Office', 'High-End', 'Laptop'], 5999.99, [1, 2, 3], 1),
        (2, 'Mechanical Keyboard', ['Electronics', 'Accessories'], 399.99, [1, 2], 1),
        (3, 'Basketball', ['Sports', 'Outdoor'], 199.99, [1,3], 1),
        (4, 'Badminton Racket', ['Sports', 'Equipment'], 299.99, [3], 3),
        (5, 'Shirt', ['Clothing', 'Office', 'Shirt'], 259.00, [4], 4);

     INSERT INTO test_array_decimal_table  VALUES
            (1, 'Laptop', ['Electronics', 'Office', 'High-End', 'Laptop'], 5999.99, [1, 2, 3], 2),
            (2, 'Mechanical Keyboard', ['Electronics', 'Accessories'], 399.99, [1, 2], 2),
            (3, 'Basketball', ['Sports', 'Outdoor'], 199.99, [1,3], 3);

     INSERT INTO test_array_decimal_table  VALUES
            (1, 'Laptop', ['Electronics', 'Office', 'High-End', 'Laptop'], 5999.99, [1, 2, 3], 3);
    """

    qt_window_function_partition_by_array_decimal """
    select
        name,
        category_ids,
        cid,
        sum(cid) over(partition by category_ids)
    from
        test_array_decimal_table
    group by
        name, category_ids, cid
    order by
        name, category_ids, cid;
    """
}
