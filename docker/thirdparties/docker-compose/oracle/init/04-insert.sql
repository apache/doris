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

insert into doris_test.student values (1, 'alice', 20, 99.5);
insert into doris_test.student values (2, 'bob', 21, 90.5);
insert into doris_test.student values (3, 'jerry', 23, 88.0);
insert into doris_test.student values (4, 'andy', 21, 93);

insert into doris_test.test_num values
(1, 111, 123, 7456123.89, 573, 34, 673.43, 34.1264, 56.2, 23.231);

insert into doris_test.test_int values
(1, 99, 9999, 999999999, 999999999999999999, 999, 99999, 9999999999, 9999999999999999999);
insert into doris_test.test_int values
(2, -99, -9999, -999999999, -999999999999999999, -999, -99999, -9999999999, -9999999999999999999);
insert into doris_test.test_int values
(3, 9.9, 99.99, 999999999, 999999999999999999, 999, 99999, 9999999999, 9999999999999999999);


insert into doris_test.test_char values (1, '1', 'china', 'beijing', 'alice', 'abcdefghrjkmnopq');
insert into doris_test.test_char values (2, '2', 'china', 'shanghai', 'bob', 'abcdefghrjkmnopq');
insert into doris_test.test_char values (3, '3', 'Americ', 'new york', 'Jerry', 'abcdefghrjkmnopq');


insert into doris_test.test_raw values (1, hextoraw('ffff'), hextoraw('aaaa'));
insert into doris_test.test_raw values (2, utl_raw.cast_to_raw('beijing'), utl_raw.cast_to_raw('shanghai'));

insert into doris_test.test_date (id, t1) values (1, to_date('2022-1-21 5:23:01','yyyy-mm-dd hh24:mi:ss'));
insert into doris_test.test_date (id, t1) values (2, to_date('20221112203256', 'yyyymmddhh24miss'));
insert into doris_test.test_date (id, t2) values (3, interval '11' year);
insert into doris_test.test_date (id, t2) values (4, interval '223-9' year(3) to month);
insert into doris_test.test_date (id, t3) values (5, interval '12 10:23:01.1234568' day to second);

insert into doris_test.test_timestamp (id, t1) values (1, to_date('2013-1-21 5:23:01','yyyy-mm-dd hh24:mi:ss'));
insert into doris_test.test_timestamp (id, t1) values (2, to_date('20131112203256', 'yyyymmddhh24miss'));
insert into doris_test.test_timestamp (id, t2) values (3, to_timestamp('20191112203357.999', 'yyyymmddhh24miss.ff'));
insert into doris_test.test_timestamp (id, t3) values (4, to_timestamp('20191112203357.999997623', 'yyyymmddhh24miss.ff'));
insert into doris_test.test_timestamp (id, t4) values (5, to_timestamp_tz('20191112203357.999996623', 'yyyymmddhh24miss.ff'));
insert into doris_test.test_timestamp (id, t5) values (6, to_timestamp_tz('20191112203357.999996623', 'yyyymmddhh24miss.ff'));
insert into doris_test.test_timestamp (id, t6) values (7, interval '11' year);
insert into doris_test.test_timestamp (id, t6) values (8, interval '223-9' year(3) to month);
insert into doris_test.test_timestamp (id, t7) values (9, interval '12 10:23:01.1234568' day to second);

insert into doris_test.test_number values (1, 123.45, 12345, 0.0012345);
insert into doris_test.test_number values (2, 123.45, 12345, 0.0099999);
insert into doris_test.test_number values (3, 123.456, 123456.12, 0.00123456);
insert into doris_test.test_number values (4, 12.3456, 1234567, 0.001234567);
insert into doris_test.test_number values (5, 123.56, 9999899, 0.009999899);

insert into doris_test.test_number2 values (1, 12345678901234567890123456789012345678);
insert into doris_test.test_number2 values (2, 99999999999999999999999999999999999999);
insert into doris_test.test_number2 values (3, 999999999999999999999999999999999999999);
insert into doris_test.test_number2 values (4, 12345678);
insert into doris_test.test_number2 values (5, 123.123);
insert into doris_test.test_number2 values (6, 0.999999999999);

insert into doris_test.test_number3 values (1, 9999);
insert into doris_test.test_number3 values (2, 12345678901234567890123456789012345678);
insert into doris_test.test_number3 values (3, 99999999999999999999999999999999999999);
insert into doris_test.test_number3 values (4, 0.99999);

insert into doris_test.test_number4 values (1, 12345678);
insert into doris_test.test_number4 values (2, 123456789012);
insert into doris_test.test_clob values (10086, 'yidong');
insert into doris_test.test_clob values (10010, 'liantong');

insert into doris_test."AA/D" values (1, 'alice', 20, 99.5);
insert into doris_test.aaad values (1, 'alice');


INSERT INTO doris_test.test_numerical_type
VALUES
(
    123.45, NULL, 3.4028235E+38, -3.4028235E+38, -1.17549435E-38,1.0E100, 123.456E10, NULL, -1.7976931348623157E+38, 1.7976931348623157E+38, NULL,1E100, 1.0, 123456.123456, NULL, 1E100, 1.0, 1234567890123456789.0123456789, NULL, 100000.0, 123000.0
);

INSERT INTO doris_test.test_vachar_type
VALUES
(
    'string 010','string 20','string max size','NULL','NULL','string 010','string 20','string max size','NULL','string 010','string 20','string max size','NULL','string 010','string 20','string max size'
);


INSERT INTO doris_test.test_nvachar_2_type
VALUES
(
'clob',NULL,'clob',NULL
);


INSERT INTO doris_test.test_clob_type
VALUES
(
'clob',NULL,empty_clob(),'clob',NULL,empty_clob()
);

INSERT INTO doris_test.test_char_type_01
VALUES
(
'string 010','string 20','string max size',NULL,NULL,'string 010','string 20','string max size',NULL,'string 010','string 20','string max size'
);

INSERT INTO doris_test.test_char_type_02
VALUES
(
NULL,'string 010','string 20','string max size'
);


INSERT INTO doris_test.test_decimal_type_01
VALUES
(
CAST(193 AS DECIMAL(3, 0)),CAST(19 AS DECIMAL(3, 0)),CAST(-193 AS DECIMAL(3, 0)),CAST(10.0 AS DECIMAL(3, 1)),CAST(10.1 AS DECIMAL(3, 1)),CAST(-10.1 AS DECIMAL(3, 1)),CAST(2 AS DECIMAL(4, 2)),CAST(2.3 AS DECIMAL(4, 2))
);

INSERT INTO doris_test.test_decimal_type_02
VALUES
(
CAST(2 AS DECIMAL(24, 2)),CAST(2.3 AS DECIMAL(24, 2)),CAST(123456789.3 AS DECIMAL(24, 2)),CAST(12345678901234567890.31 AS DECIMAL(24, 4)),CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5)),CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))
);

INSERT INTO doris_test.test_decimal_2_type
VALUES
(
193,19,-193,10.0,10.1,-10.1,2,2.3,2,2.3,123456789.3,12345678901234567890.31,3141592653589793238462643.38327,-3141592653589793238462643.38327,'27182818284590452353602874713526624977','-27182818284590452353602874713526624977',NULL,NULL,NULL
);

INSERT INTO doris_test.test_number_type
VALUES
(
1,99,99999999999999999999999999999999999999,-99999999999999999999999999999999999999,1,99,9999999999999999999999999999.999999999,-9999999999999999999999999999.999999999,99999999999999999999999999999999999990,-99999999999999999999999999999999999990,CAST('99999999999999999999999999999999999990' AS DECIMAL(38, 0)),CAST('-99999999999999999999999999999999999990' AS DECIMAL(38, 0)),20,35,470000,-80000,-8.8888888E+10,4050000,1.4000014000014E+27,1E+21,1.2345E+37,-1.2345E+37,1E+37,-1E+37,0.0012345678901234567890123456789012345678,0.0000000000000000000000123456789012345678,0.00000000000000000000000000000000000000000000012345678901234567890123456789012345678,1234567890123456789012345678901234567000000000000000000000000000000000000000000000000000000000000,0.000000000000000000000000000000000000000000000123456789012345678,12345678901234567890.12345678901234567890123456789012345678
);

INSERT INTO doris_test.test_blob_type
VALUES
(
    NULL,empty_blob(),hextoraw('68656C6C6F'),hextoraw('5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'), hextoraw('4261672066756C6C206F6620F09F92B0'),hextoraw('0001020304050607080DF9367AA7000000'),hextoraw('000000000000')
);


INSERT INTO doris_test.test_raw_type
VALUES
(
    NULL,empty_blob(),hextoraw('68656C6C6F'),hextoraw('5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'),hextoraw('4261672066756C6C206F6620F09F92B0'),hextoraw('0001020304050607080DF9367AA7000000')
);


INSERT INTO doris_test_02.nation VALUES (0,'ALGERIA', 0,'haggle. carefully final deposits detect slyly agai');
INSERT INTO doris_test_02.nation VALUES (1,'ARGENTINA',1,'al foxes promise slyly according to the regular accounts. bold requests alon');
INSERT INTO doris_test_02.nation VALUES (2,'BRAZIL',1,'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special' );
INSERT INTO doris_test_02.nation VALUES (3,'CANADA',1,'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold');
INSERT INTO doris_test_02.nation VALUES (4,'EGYPT',4,'y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d');
INSERT INTO doris_test_02.nation VALUES (5,'ETHIOPIA',0,'ven packages wake quickly. regu');
INSERT INTO doris_test_02.nation VALUES (6,'FRANCE',3,'refully final requests. regular, ironi');
INSERT INTO doris_test_02.nation VALUES (7,'GERMANY',3,'l platelets. regular accounts x-ray: unusual, regular acco');
INSERT INTO doris_test_02.nation VALUES (8,'INDIA',2,'ss excuses cajole slyly across the packages. deposits print aroun');
INSERT INTO doris_test_02.nation VALUES (9,'INDONESIA',2, 'slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull');


INSERT INTO doris_test.nation_load VALUES (0,'ALGERIA', 0,'haggle. carefully final deposits detect slyly agai');
INSERT INTO doris_test.nation_load VALUES (1,'ARGENTINA',1,'al foxes promise slyly according to the regular accounts. bold requests alon');
INSERT INTO doris_test.nation_load VALUES (2,'BRAZIL',1,'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special' );
INSERT INTO doris_test.nation_load VALUES (3,'CANADA',1,'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold');
INSERT INTO doris_test.nation_load VALUES (4,'EGYPT',4,'y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d');
INSERT INTO doris_test.nation_load VALUES (5,'ETHIOPIA',0,'ven packages wake quickly. regu');
INSERT INTO doris_test.nation_load VALUES (6,'FRANCE',3,'refully final requests. regular, ironi');
INSERT INTO doris_test.nation_load VALUES (7,'GERMANY',3,'l platelets. regular accounts x-ray: unusual, regular acco');
INSERT INTO doris_test.nation_load VALUES (8,'INDIA',2,'ss excuses cajole slyly across the packages. deposits print aroun');
INSERT INTO doris_test.nation_load VALUES (9,'INDONESIA',2, 'slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull');

INSERT INTO doris_test.nation_read_and_write VALUES (0,'ALGERIA', 0,'haggle. carefully final deposits detect slyly agai');
INSERT INTO doris_test.nation_read_and_write VALUES (1,'ARGENTINA',1,'al foxes promise slyly according to the regular accounts. bold requests alon');
INSERT INTO doris_test.nation_read_and_write VALUES (2,'BRAZIL',1,'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special' );
INSERT INTO doris_test.nation_read_and_write VALUES (3,'CANADA',1,'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold');
INSERT INTO doris_test.nation_read_and_write VALUES (4,'EGYPT',4,'y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d');
INSERT INTO doris_test.nation_read_and_write VALUES (5,'ETHIOPIA',0,'ven packages wake quickly. regu');
INSERT INTO doris_test.nation_read_and_write VALUES (6,'FRANCE',3,'refully final requests. regular, ironi');
INSERT INTO doris_test.nation_read_and_write VALUES (7,'GERMANY',3,'l platelets. regular accounts x-ray: unusual, regular acco');
INSERT INTO doris_test.nation_read_and_write VALUES (8,'INDIA',2,'ss excuses cajole slyly across the packages. deposits print aroun');
INSERT INTO doris_test.nation_read_and_write VALUES (9,'INDONESIA',2, 'slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull');

insert into doris_test.student2 values (1, 'alice', 20, 99.5);
insert into doris_test.student2 values (2, 'bob', 21, 90.5);
insert into doris_test.student2 values (3, 'jerry', 23, 88.0);
insert into doris_test.student2 values (4, 'andy', 21, 93);


commit;

