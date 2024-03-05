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

CREATE TABLE doris_test.sample_table (
    id_column INT GENERATED ALWAYS AS IDENTITY,
    numeric_column NUMERIC,
    decimal_column DECIMAL(31, 10),
    decfloat_column DECFLOAT,
    float_column FLOAT,
    real_column REAL,
    double_column DOUBLE,
    double_precision_column DOUBLE PRECISION,
    smallint_column SMALLINT,
    int_column INT,
    bigint_column BIGINT,
    varchar_column VARCHAR(255),
    varcharphic_column VARGRAPHIC(50),
    long_varchar_column LONG VARCHAR ,
    long_varcharphic_column LONG VARGRAPHIC,
    char_varying_column CHAR VARYING(255),
    char_column CHAR(255),
    date_column DATE,
    timestamp_column TIMESTAMP,
    time_column TIME,
    clob_column CLOB
);
