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

INSERT INTO doris_test.type VALUES
(true, '2022-01-01','2022-01-01','2022-01-01 00:00:00','2022-01-01 00:00:00.000000000',1.1,1.1,1,1,1,1,1,1,1,1,1,1,1,1,1.1,1.1,1.1,1.1,1,'116.253.40.133','2a02:aa08:e000:3100::2','61f0c404-5cb3-11e7-907b-a6006ad3dba0','String','F');
INSERT INTO doris_test.type VALUES
(false, '2022-01-02','2022-01-02','2022-01-02 00:00:00','2022-01-02 00:00:00.000000000',2.2,2.2,2,2,2,2,2,2,2,2,2,2,2,2,2.2,2.2,2.2,2.2,2,'116.253.40.133','2a02:aa08:e000:3100::2','61f0c404-5cb3-11e7-907b-a6006ad3dba0','String','T');


INSERT INTO doris_test.type_null VALUES
('1',true, '2022-01-01','2022-01-01','2022-01-01 00:00:00','2022-01-01 00:00:00.000000000',1.1,1.1,1,1,1,1,1,1,1,1,1,1,1,1,1.1,1.1,1.1,1.1,1,'116.253.40.133','2a02:aa08:e000:3100::2','61f0c404-5cb3-11e7-907b-a6006ad3dba0','String','F');
INSERT INTO doris_test.type_null VALUES
('2',false, '2022-01-02','2022-01-02','2022-01-02 00:00:00','2022-01-02 00:00:00.000000000',2.2,2.2,2,2,2,2,2,2,2,2,2,2,2,2,2.2,2.2,2.2,2.2,2,'116.253.40.133','2a02:aa08:e000:3100::2','61f0c404-5cb3-11e7-907b-a6006ad3dba0','String','T');
INSERT INTO doris_test.type_null VALUES
('3',NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO doris_test.number
(`k6`, `k7`, `k8`, `k9`, `k10`, `k11`, `k12`, `k13`, `k14`, `k15`, `k16`, `k17`, `k18`, `k19`)
VALUES
    (-3.4028235e38, -1.7976931348623157e308, -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728, -57896044618658097711785492504343953926634992332820282019728792003956564819968, 0, 0, 0, 0, 0, 0);
INSERT INTO doris_test.number
(`k6`, `k7`, `k8`, `k9`, `k10`, `k11`, `k12`, `k13`, `k14`, `k15`, `k16`, `k17`, `k18`, `k19`)
VALUES
    (3.4028235e38, 1.7976931348623157e308, 127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727, 57896044618658097711785492504343953926634992332820282019728792003956564819967, 255, 65535, 4294967295, 18446744073709551615, 340282366920938463463374607431768211455, 115792089237316195423570985008687907853269984665640564039457584007913129639935);

INSERT INTO doris_test.student values (1, 'doris', 18), (2, 'alice', 19), (3, 'bob', 20);

INSERT INTO doris_test.arr values
('1',[true],['2022-01-01'],['2022-01-01'],[1.1],[1.1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[2.2],[1],['116.253.40.133'],['2a02:aa08:e000:3100::2'],['61f0c404-5cb3-11e7-907b-a6006ad3dba0'],[1],['string'],['string'],['2022-01-01 00:00:00'],['2022-01-01 00:00:00']);

INSERT INTO doris_test.arr_null values
('1',[true],['2022-01-01'],['2022-01-01'],[1.1],[1.1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[2.2],[1],['116.253.40.133'],['2a02:aa08:e000:3100::2'],['61f0c404-5cb3-11e7-907b-a6006ad3dba0'],[1],['string'],['string'],['2022-01-01 00:00:00'],['2022-01-01 00:00:00']);
INSERT INTO doris_test.arr_null values
('2', [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL]);

INSERT INTO doris_test.json VALUES ('1','{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}');

INSERT INTO doris_test.final_test Values (1, 'first');
INSERT INTO doris_test.final_test Values (1, 'second');

INSERT INTO doris_test.ts values (1,1694438743);

INSERT INTO doris_test.dt_with_tz values(1, '2022-01-02 00:00:00','2022-01-02 00:00:00.000000');

INSERT INTO doris_test.extreme_test VALUES (1,127,127,32767,32767,2147483647,2147483647,9223372036854775807,9223372036854775807,255,255,65535,65535,4294967295,4294967295,18446744073709551615,18446744073709551615,3.4028235e38,3.4028235e38,1.7976931348623157e308,1.7976931348623157e308,9999999999999999.99,9999999999999999.99,'2106-02-07','2106-02-07','2106-02-07 06:28:15','2106-02-07 06:28:15','max_string','max_string','XXXXXXXXXX','XXXXXXXXXX','C','C','ffffffff-ffff-ffff-ffff-ffffffffffff','ffffffff-ffff-ffff-ffff-ffffffffffff','255.255.255.255','255.255.255.255','ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff','ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff');
INSERT INTO doris_test.extreme_test VALUES (2,-128,-128,-32768,-32768,-2147483648,-2147483648,-9223372036854775808,-9223372036854775808,0,0,0,0,0,0,0,0,-3.4028235e38,-3.4028235e38,-1.7976931348623157e308,-1.7976931348623157e308,-9999999999999999.99,-9999999999999999.99,'1970-01-01','1970-01-01','1970-01-01 00:00:00','1970-01-01 00:00:00','min_string','min_string','0000000000','0000000000','A','A','00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000','0.0.0.0','0.0.0.0','0000:0000:0000:0000:0000:0000:0000:0000','0000:0000:0000:0000:0000:0000:0000:0000');
INSERT INTO doris_test.extreme_test VALUES (3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'1970-01-01','1970-01-01','1970-01-01 00:00:00','1970-01-01 00:00:00','','','','','A','A','00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000','0.0.0.0','0.0.0.0','0000:0000:0000:0000:0000:0000:0000:0000','0000:0000:0000:0000:0000:0000:0000:0000');
INSERT INTO doris_test.extreme_test VALUES (4,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,0,NULL,'1970-01-01',NULL,'1970-01-01 00:00:00',NULL,'',NULL,'',NULL,'A',NULL,'00000000-0000-0000-0000-000000000000',NULL,'0.0.0.0',NULL,'0000:0000:0000:0000:0000:0000:0000:0000',NULL);

insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test;
insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test_multi_block;
insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test_multi_block;
insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test_multi_block;
insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test_multi_block;
insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test_multi_block;
insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test_multi_block;
insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test_multi_block;
insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test_multi_block;
insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test_multi_block;
insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test_multi_block;
insert into doris_test.extreme_test_multi_block select * from doris_test.extreme_test;
