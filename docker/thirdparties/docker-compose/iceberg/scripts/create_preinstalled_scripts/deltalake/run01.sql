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

-- Delta Lake test data initialization
-- This script creates the delta_test database and tables
-- that are expected by the regression tests.

use delta_lake;

create database if not exists delta_test;
use delta_test;

-- ==========================================================
-- Table: delta_basic
-- A basic Delta Lake table with primitive types
-- ==========================================================
drop table if exists delta_basic;
CREATE TABLE delta_basic (
    id INT,
    name STRING,
    value DOUBLE
) USING delta
LOCATION 's3a://warehouse/wh/delta_test/delta_basic';

INSERT INTO delta_basic VALUES
    (1, 'Alice', 10.5),
    (2, 'Bob', 20.3),
    (3, 'Charlie', 30.7),
    (4, 'David', 40.1),
    (5, 'Eve', 50.9),
    (6, 'Frank', 60.2),
    (7, 'Grace', 70.8),
    (8, 'Heidi', 80.4),
    (9, 'Ivan', 90.6),
    (10, 'Judy', 100.0);

-- ==========================================================
-- Table: delta_partitioned
-- A Delta Lake table with partition column
-- ==========================================================
drop table if exists delta_partitioned;
CREATE TABLE delta_partitioned (
    id INT,
    name STRING,
    dt STRING
) USING delta
PARTITIONED BY (dt)
LOCATION 's3a://warehouse/wh/delta_test/delta_partitioned';

INSERT INTO delta_partitioned VALUES
    (1, 'Alice', '2024-01-01'),
    (2, 'Bob', '2024-01-01'),
    (3, 'Charlie', '2024-01-01'),
    (4, 'David', '2024-01-02'),
    (5, 'Eve', '2024-01-02'),
    (6, 'Frank', '2024-01-03');

-- ==========================================================
-- Table: delta_with_dv
-- A Delta Lake table with Deletion Vectors enabled
-- Write data then delete some rows to trigger DV creation
-- ==========================================================
drop table if exists delta_with_dv;
CREATE TABLE delta_with_dv (
    id INT,
    data STRING
) USING delta
LOCATION 's3a://warehouse/wh/delta_test/delta_with_dv'
TBLPROPERTIES ('delta.enableDeletionVectors' = true);

INSERT INTO delta_with_dv VALUES
    (1, 'keep_a'),
    (2, 'delete_b'),
    (3, 'keep_c'),
    (4, 'delete_d'),
    (5, 'keep_e');

DELETE FROM delta_with_dv WHERE id IN (2, 4);
