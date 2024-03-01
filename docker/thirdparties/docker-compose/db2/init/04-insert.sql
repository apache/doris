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

-- Insert normal data
INSERT INTO doris_test.sample_table (
    numeric_column,
    decimal_column,
    decfloat_column,
    float_column,
    real_column,
    double_column,
    double_precision_column,
    smallint_column,
    int_column,
    bigint_column,
    varchar_column,
    varcharphic_column,
    long_varchar_column,
    long_varcharphic_column,
    char_varying_column,
    char_column,
    date_column,
    timestamp_column,
    time_column,
    clob_column
) VALUES (
    123,
    1234567890.1234567890,
    1.234567890,
    12345.6789,
    12345.6789,
    1234567890.1234567890,
    1234567890.1234567890,
    123,
    12345,
    123456789012345,
    'Varchar text',
    'Varcharphic text',
    'Long varchar text',
    'Long varcharphic text',
    'Char varying text',
    'Char text',
    '2024-01-24',
    '2024-01-24-12.34.56.789000',
    '12:34:56',
    'Sample CLOB text'
);

-- Insert null data
INSERT INTO doris_test.sample_table (
    numeric_column,
    decimal_column,
    decfloat_column,
    float_column,
    real_column,
    double_column,
    double_precision_column,
    smallint_column,
    int_column,
    bigint_column,
    varchar_column,
    varcharphic_column,
    long_varchar_column,
    long_varcharphic_column,
    char_varying_column,
    char_column,
    date_column,
    timestamp_column,
    time_column,
    clob_column
) VALUES (
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
);

INSERT INTO doris_test.sample_table (
    numeric_column,
    decimal_column,
    decfloat_column,
    float_column,
    real_column,
    double_column,
    double_precision_column,
    smallint_column,
    int_column,
    bigint_column,
    varchar_column,
    varcharphic_column,
    long_varchar_column,
    long_varcharphic_column,
    char_varying_column,
    char_column,
    date_column,
    timestamp_column,
    time_column,
    clob_column
) VALUES (
    123,
    1234567890.1234567890,
    1.234567890,
    12345.6789,
    12345.6789,
    1234567890.1234567890,
    1234567890.1234567890,
    123,
    12345,
    123456789012345,
    '中文一',
    '中文二',
    '中文三',
    '中文四',
    '中文五',
    '中文六',
    '2024-01-24',
    '2024-01-24-12.34.56.789000',
    '12:34:56',
    '中文七'
);
