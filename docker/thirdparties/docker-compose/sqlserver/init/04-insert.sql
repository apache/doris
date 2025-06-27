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
UPDATE STATISTICS dbo.student;

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

Insert into dbo.TEST_LOWER values (1, 'doris');
UPDATE STATISTICS dbo.TEST_LOWER;

INSERT INTO dbo.extreme_test (ID,Int8_Col,Int8_Nullable,Int16_Col,Int16_Nullable,Int32_Col,Int32_Nullable,Int64_Col,Int64_Nullable,UInt8_Col,UInt8_Nullable,UInt16_Col,UInt16_Nullable,UInt32_Col,UInt32_Nullable,UInt64_Col,UInt64_Nullable,Float32_Col,Float32_Nullable,Float64_Col,Float64_Nullable,Decimal_Col1,Decimal_Nullable1,Decimal_Col2,Decimal_Nullable2,Decimal_Col3,Decimal_Nullable3,Date_Col,Date_Nullable,Datetime_Col,Datetime_Nullable,String_Col,String_Nullable,FixedString_Col,FixedString_Nullable,Enum_Col,Enum_Nullable,UUID_Col,UUID_Nullable,IPv4_Col,IPv4_Nullable,IPv6_Col,IPv6_Nullable) VALUES (1,127,127,32767,32767,2147483647,2147483647,9223372036854775807,9223372036854775807,255,255,65535,65535,4294967295,4294967295,18446744073709551615,18446744073709551615,3.4028235e38,3.4028235e38,1.7976931348623157e308,1.7976931348623157e308,9999999999999999.99,9999999999999999.99,99999.99999,99999.99999,9999999999999999999999999999.9999999999,9999999999999999999999999999.9999999999,'9999-12-31','9999-12-31','9999-12-31 23:59:59','9999-12-31 23:59:59','max_string','max_string','XXXXXXXXXX','XXXXXXXXXX','C','C','FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF','FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF','255.255.255.255','255.255.255.255','FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF','FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF');
INSERT INTO dbo.extreme_test (ID,Int8_Col,Int8_Nullable,Int16_Col,Int16_Nullable,Int32_Col,Int32_Nullable,Int64_Col,Int64_Nullable,UInt8_Col,UInt8_Nullable,UInt16_Col,UInt16_Nullable,UInt32_Col,UInt32_Nullable,UInt64_Col,UInt64_Nullable,Float32_Col,Float32_Nullable,Float64_Col,Float64_Nullable,Decimal_Col1,Decimal_Nullable1,Decimal_Col2,Decimal_Nullable2,Decimal_Col3,Decimal_Nullable3,Date_Col,Date_Nullable,Datetime_Col,Datetime_Nullable,String_Col,String_Nullable,FixedString_Col,FixedString_Nullable,Enum_Col,Enum_Nullable,UUID_Col,UUID_Nullable,IPv4_Col,IPv4_Nullable,IPv6_Col,IPv6_Nullable) VALUES (2,-128,-128,-32768,-32768,-2147483648,-2147483648,-9223372036854775808,-9223372036854775808,0,0,0,0,0,0,0,0,-3.4028235e38,-3.4028235e38,-1.7976931348623157e308,-1.7976931348623157e308,-9999999999999999.99,-9999999999999999.99,-99999.99999,-99999.99999,-9999999999999999999999999999.9999999999,-9999999999999999999999999999.9999999999,'0001-01-01','0001-01-01','1753-01-01 00:00:00','1753-01-01 00:00:00','min_string','min_string','0000000000','0000000000','A','A','00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000','0.0.0.0','0.0.0.0','0000:0000:0000:0000:0000:0000:0000:0000','0000:0000:0000:0000:0000:0000:0000:0000');
INSERT INTO dbo.extreme_test (ID,Int8_Col,Int8_Nullable,Int16_Col,Int16_Nullable,Int32_Col,Int32_Nullable,Int64_Col,Int64_Nullable,UInt8_Col,UInt8_Nullable,UInt16_Col,UInt16_Nullable,UInt32_Col,UInt32_Nullable,UInt64_Col,UInt64_Nullable,Float32_Col,Float32_Nullable,Float64_Col,Float64_Nullable,Decimal_Col1,Decimal_Nullable1,Decimal_Col2,Decimal_Nullable2,Decimal_Col3,Decimal_Nullable3,Date_Col,Date_Nullable,Datetime_Col,Datetime_Nullable,String_Col,String_Nullable,FixedString_Col,FixedString_Nullable,Enum_Col,Enum_Nullable,UUID_Col,UUID_Nullable,IPv4_Col,IPv4_Nullable,IPv6_Col,IPv6_Nullable) VALUES (3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'0001-01-01','0001-01-01','1753-01-01 00:00:00','1753-01-01 00:00:00','','','', '', 'A','A','00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000','0.0.0.0','0.0.0.0','0000:0000:0000:0000:0000:0000:0000:0000','0000:0000:0000:0000:0000:0000:0000:0000');
INSERT INTO dbo.extreme_test (ID,Int8_Col,Int8_Nullable,Int16_Col,Int16_Nullable,Int32_Col,Int32_Nullable,Int64_Col,Int64_Nullable,UInt8_Col,UInt8_Nullable,UInt16_Col,UInt16_Nullable,UInt32_Col,UInt32_Nullable,UInt64_Col,UInt64_Nullable,Float32_Col,Float32_Nullable,Float64_Col,Float64_Nullable,Decimal_Col1,Decimal_Nullable1,Decimal_Col2,Decimal_Nullable2,Decimal_Col3,Decimal_Nullable3,Date_Col,Date_Nullable,Datetime_Col,Datetime_Nullable,String_Col,String_Nullable,FixedString_Col,FixedString_Nullable,Enum_Col,Enum_Nullable,UUID_Col,UUID_Nullable,IPv4_Col,IPv4_Nullable,IPv6_Col,IPv6_Nullable) VALUES (4,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,'0001-01-01',NULL,'1753-01-01 00:00:00',NULL,'',NULL,'',NULL,'A',NULL,'00000000-0000-0000-0000-000000000000',NULL,'0.0.0.0',NULL,'0000:0000:0000:0000:0000:0000:0000:0000',NULL);

insert into dbo.extreme_test_multi_block select * from dbo.extreme_test;
insert into dbo.extreme_test_multi_block select * from dbo.extreme_test_multi_block;
insert into dbo.extreme_test_multi_block select * from dbo.extreme_test_multi_block;
insert into dbo.extreme_test_multi_block select * from dbo.extreme_test_multi_block;
insert into dbo.extreme_test_multi_block select * from dbo.extreme_test_multi_block;
insert into dbo.extreme_test_multi_block select * from dbo.extreme_test_multi_block;
insert into dbo.extreme_test_multi_block select * from dbo.extreme_test_multi_block;
insert into dbo.extreme_test_multi_block select * from dbo.extreme_test_multi_block;
insert into dbo.extreme_test_multi_block select * from dbo.extreme_test_multi_block;
insert into dbo.extreme_test_multi_block select * from dbo.extreme_test_multi_block;
insert into dbo.extreme_test_multi_block select * from dbo.extreme_test_multi_block;
insert into dbo.extreme_test_multi_block select * from dbo.extreme_test;

INSERT INTO dbo.test_identity_decimal(col) select 1;
