-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Delta Lake test data initialization - data types
-- This script creates tables with various data types

use delta_lake;
use delta_test;

-- ==========================================================
-- Table: delta_all_types
-- A Delta Lake table testing various data types
-- ==========================================================
drop table if exists delta_all_types;
CREATE TABLE delta_all_types (
    col_boolean BOOLEAN,
    col_tinyint TINYINT,
    col_smallint SMALLINT,
    col_int INT,
    col_bigint BIGINT,
    col_float FLOAT,
    col_double DOUBLE,
    col_decimal DECIMAL(18,6),
    col_string STRING,
    col_date DATE,
    col_timestamp TIMESTAMP
) USING delta
LOCATION 's3a://warehouse/wh/delta_test/delta_all_types';

INSERT INTO delta_all_types VALUES
    (true, 1, 100, 10000, 1000000000, 1.1, 1.11, 123456.789012, 'hello', DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00'),
    (false, 2, 200, 20000, 2000000000, 2.2, 2.22, 654321.098765, 'world', DATE '2024-06-15', TIMESTAMP '2024-06-15 12:30:00'),
    (true, 3, 300, 30000, 3000000000, 3.3, 3.33, 111111.111111, 'delta', DATE '2024-12-31', TIMESTAMP '2024-12-31 23:59:59'),
    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- ==========================================================
-- Table: delta_complex_types
-- A Delta Lake table with complex types (array, map, struct)
-- ==========================================================
drop table if exists delta_complex_types;
CREATE TABLE delta_complex_types (
    id INT,
    col_array ARRAY<INT>,
    col_map MAP<STRING, INT>,
    col_struct STRUCT<x: INT, y: STRING>
) USING delta
LOCATION 's3a://warehouse/wh/delta_test/delta_complex_types';

INSERT INTO delta_complex_types VALUES
    (1, ARRAY(1, 2, 3), MAP('a', 10, 'b', 20), NAMED_STRUCT('x', 100, 'y', 'foo')),
    (2, ARRAY(4, 5), MAP('c', 30), NAMED_STRUCT('x', 200, 'y', 'bar')),
    (3, ARRAY(), MAP(), NAMED_STRUCT('x', 300, 'y', 'baz')),
    (4, NULL, NULL, NULL);

-- ==========================================================
-- Table: delta_nested_types
-- A Delta Lake table with nested complex types
-- ==========================================================
drop table if exists delta_nested_types;
CREATE TABLE delta_nested_types (
    id INT,
    col_array_of_struct ARRAY<STRUCT<name: STRING, value: INT>>,
    col_map_of_array MAP<STRING, ARRAY<INT>>
) USING delta
LOCATION 's3a://warehouse/wh/delta_test/delta_nested_types';

INSERT INTO delta_nested_types VALUES
    (1, ARRAY(NAMED_STRUCT('name', 'a', 'value', 1), NAMED_STRUCT('name', 'b', 'value', 2)), MAP('k1', ARRAY(1,2,3), 'k2', ARRAY(4,5))),
    (2, ARRAY(NAMED_STRUCT('name', 'c', 'value', 3)), MAP('k3', ARRAY(6,7,8)));
