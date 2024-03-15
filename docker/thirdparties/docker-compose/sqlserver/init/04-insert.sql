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
use doris_test;

Insert into dbo.student values (1, 'doris', 18), (2, 'alice', 19), (3, 'bob', 20);

Insert into dbo.test_int values
(1, 0, 1, 1), (2, 1, -1, -1),
(3, 255, 32767, 9223372036854775807), (4, 128, -32768, -9223372036854775808);

Insert into dbo.test_float values
(1, 123.123, 123.123, 123.123, 123.123, 123.123),
(2, 12345.12345, 12345.12345, 12345.12345, 12345.12345,12345.12345),
(3, -123.123, -123.123, -123.123, -123.123, -123.123);

Insert into dbo.test_decimal values
(1, 12345678901234567890123456789012345678, 12345678901234567890123456789012345678, 1234567890123456789012345678.0123456789, 1234567890123456789012345678.0123456789),
(2, -12345678901234567890123456789012345678, -12345678901234567890123456789012345678, -1234567890123456789012345678.0123456789, -1234567890123456789012345678.0123456789);

Insert into dbo.test_char values
(1, 'Make Doris Great!', 'Make Doris Great!', 'Make Doris Great!', 'Make Doris Great!', 'Make Doris Great!', 'Make Doris Great!');

Insert into dbo.test_time values (1, '2023-01-17', '16:49:05.1234567', '2023-01-17 16:49:05', '2023-01-17 16:49:05.1234567', '2023-01-17 16:49:05', '2023-01-17 16:49:05+08:00'),
(2, '2023-01-17', '16:49:05', '2023-01-17 16:49:05', '2023-01-17 16:49:05', '2023-01-17 16:49:05', '2023-01-17 16:49:05+08:00');
Insert into dbo.test_time values (3, '2023-01-17', '16:49:05.1234567', '2023-01-17 16:49:05', '2023-01-17 16:49:05.1234567', '2023-01-17 16:49:05', '2023-01-17 16:49:05.1234567+08:00');
Insert into dbo.test_time values (4, '2023-01-17', '16:49:05', '2023-01-17 16:49:05', '2023-01-17 16:49:05', '2023-01-17 16:49:05', '2023-01-17 16:49:05+08:00');

Insert into dbo.test_text values (1, 'Make Doris Great!', 'Make Doris Great!');

Insert into dbo.test_money values (1, 922337203685477.5807, 214748.3647);
Insert into dbo.test_money values (2, -922337203685477.5808, -214748.3648);
Insert into dbo.test_money values (3, 123.123, 123.123);

insert into dbo.test_binary values (1, 0, 0x4D616B6520446F72697320477265617421, 0x4D616B6520446F72697320477265617421);
insert into dbo.test_binary values (2, 1, 0x4D616B6520446F72697320477265617421, 0x4D616B6520446F72697320477265617421);
insert into dbo.test_binary values (3, -1, 0x4D616B6520446F72697320477265617421, 0x4D616B6520446F72697320477265617421);


INSERT INTO dbo.DateAndTime
VALUES (
    '2023-06-25', -- DATE
    '14:30:45', -- TIME
    '2023-06-25T14:30:45', -- DATETIME
    '2023-06-25T14:30:00', -- SMALLDATETIME
    '2023-06-25T14:30:45.1234567', -- DATETIME2
    '2023-06-25 14:30:45.1234567 -07:00' -- DATETIMEOFFSET
);

INSERT INTO dbo.t_id (ID, Name) VALUES (NEWID(), 'Data 1');
INSERT INTO dbo.t_id (ID, Name) VALUES (NEWID(), 'Data 2');

Insert into dbo.all_type values
(
1,
'doris',
18,
0,
1,
1,
123.123,
123.123,
123.123,
12345678901234567890123456789012345678,
12345678901234567890123456789012345678,
1234567890123456789012345678.0123456789,
1234567890123456789012345678.0123456789,
'Make Doris Great!',
'Make Doris Great!',
'Make Doris Great!',
'Make Doris Great!',
'Make Doris Great!',
'Make Doris Great!',
'2023-01-17',
'16:49:05.1234567',
'2023-01-17 16:49:05',
'2023-01-17 16:49:05.1234567',
'2023-01-17 16:49:05',
'2023-01-17 16:49:05+08:00',
'Make Doris Great!',
'Make Doris Great!',
922337203685477.5807,
214748.3647,
0
),
(2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

insert into dbo.test_timestamp(id_col) values(1);
