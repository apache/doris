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

INSERT INTO doris_test.test1 (
    id,
    c_ascii,
    c_bigint,
    c_boolean,
    c_decimal,
    c_double,
    c_float,
    c_int,
    c_timestamp,
    c_uuid,
    c_text,
    c_varint,
    c_timeuuid,
    c_inet,
    c_date,
    c_smallint,
    c_tinyint,
    c_list,
    c_set,
    c_map
) VALUES (
    1,
    'sample_ascii',
    1234567890,
    true,
    123.45,
    3.14159,
    2.71828,
    42,
    '2023-09-05 12:00:00',
    123e4567-e89b-12d3-a456-426614174000,
    'Sample text',
    987654321,
    01b9fc02-7e7e-11ec-94c8-0242ac130003,
    '192.168.1.1',
    '2023-09-05',
    32767,
    127,
    [1, 2, 3],
    {4, 5, 6},
    {'key1': 100, 'key2': 200}
);
