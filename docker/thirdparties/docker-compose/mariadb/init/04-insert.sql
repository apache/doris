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
insert into doris_test.ex_tb0 values (111, 'abc'), (112, 'abd'), (113, 'abe'),(114, 'abf'),(115, 'abg');

INSERT INTO doris_test.all_types VALUES
(201, 301, 401, 501, 601, 3.14159, 4.1415926, 5.141592, true, -123, -301, 2012, -401, -501, -601, '2012-10-30', '2012-10-25 12:05:36.3456712', '2012-10-25 08:08:08.3456712',
 -4.14145001, -5.1400000001, -6.140000001, 'row1', 'line1', '09:09:09.56782346', 'text1', X'48656C6C6F20576F726C64', '{"name": "Alice", "age": 30, "city": "London"}',
 'Option1,Option3', b'101010', X'48656C6C6F', X'48656C6C6F', 'Value2'),
(202, 302, 402, 502, 602, 4.14159, 5.1415926, 6.141592, false, -124, -302, 2013, -402, -502, -602, '2012-11-01', '2012-10-26 02:08:39.3456712', '2013-10-26 08:09:18.3456712',
 -5.14145001, -6.1400000001, -7.140000001, 'row2', 'line2', '09:11:09.56782346', 'text2', X'E86F6C6C6F20576F726C67', '{"name": "Gaoxin", "age": 18, "city": "ChongQing"}',
 'Option1,Option2', b'101111', X'58676C6C6F', X'88656C6C9F', 'Value3'),
(null, 302, null, 502, 602, 4.14159, null, 6.141592, null, -124, -302, 2013, -402, -502, -602, null, '2012-10-26 02:08:39.3456712', '2013-10-26 08:09:18.3456712',
 -5.14145001, null, -7.140000001, 'row2', null, '09:11:09.56782346', 'text2', X'E86F6C6C6F20576F726C67', null,
 null, b'101111', null, X'88656C6C9F', 'Value3'),
(203, 303, 403, 503, 603, 7.14159, 8.1415926, 9.141592, false, null, -402, 2017, -602, -902, -1102, '2012-11-02', null, '2013-10-27 08:11:18.3456712',
 -5.14145000001, -6.1400000000001, -7.140000000001, 'row3', 'line3', '09:11:09.56782346', 'text3', X'E86F6C6C6F20576F726C67', '{"name": "ChenQi", "age": 24, "city": "ChongQing"}',
 'Option2', b'101111', X'58676C6C6F', null, 'Value1');

INSERT INTO doris_test.dt (`timestamp0`, `timestamp1`, `timestamp2`, `timestamp3`, `timestamp4`, `timestamp5`, `timestamp6`,`timestamp7`)
VALUES ('2023-06-17 10:00:00', '2023-06-17 10:00:01.1', '2023-06-17 10:00:02.22', '2023-06-17 10:00:03.333', 
        '2023-06-17 10:00:04.4444', '2023-06-17 10:00:05.55555', '2023-06-17 10:00:06.666666','2023-06-17 10:00:06.666666');

