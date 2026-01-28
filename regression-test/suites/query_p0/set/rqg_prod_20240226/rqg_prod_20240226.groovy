// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("rqg_prod_20240226") {
    sql """
    DROP TABLE IF EXISTS `table_200_undef_partitions2_keys3_properties4_distributed_by511`;
    """
    sql """
CREATE TABLE `table_200_undef_partitions2_keys3_properties4_distributed_by511` (
  `col_tinyint_undef_signed_not_null` tinyint NOT NULL,
  `col_decimal_5_0__undef_signed_not_null` decimal(5,0) NOT NULL,
  `col_boolean_undef_signed` boolean NULL,
  `col_boolean_undef_signed_not_null` boolean NOT NULL,
  `col_tinyint_undef_signed` tinyint NULL,
  `col_smallint_undef_signed` smallint NULL,
  `col_smallint_undef_signed_not_null` smallint NOT NULL,
  `col_int_undef_signed` int NULL,
  `col_int_undef_signed_not_null` int NOT NULL,
  `col_bigint_undef_signed` bigint NULL,
  `col_bigint_undef_signed_not_null` bigint NOT NULL,
  `col_largeint_undef_signed` largeint NULL,
  `col_largeint_undef_signed_not_null` largeint NOT NULL,
  `col_decimal_5_0__undef_signed` decimal(5,0) NULL,
  `col_decimal_12_2__undef_signed` decimal(12,2) NULL,
  `col_decimal_12_2__undef_signed_not_null` decimal(12,2) NOT NULL,
  `col_decimal_32_6__undef_signed` decimal(32,6) NULL,
  `col_decimal_32_6__undef_signed_not_null` decimal(32,6) NOT NULL,
  `col_date_undef_signed` date NULL,
  `col_date_undef_signed_not_null` date NOT NULL,
  `col_datetime_undef_signed` datetime NULL,
  `col_datetime_undef_signed_not_null` datetime NOT NULL,
  `col_datetime_6__undef_signed` datetime(6) NULL,
  `col_datetime_6__undef_signed_not_null` datetime(6) NOT NULL,
  `col_char_50__undef_signed` char(50) NULL,
  `col_char_50__undef_signed_not_null` char(50) NOT NULL,
  `col_varchar_100__undef_signed` varchar(100) NULL,
  `col_varchar_100__undef_signed_not_null` varchar(100) NOT NULL,
  `col_string_undef_signed` text NULL,
  `col_string_undef_signed_not_null` text NOT NULL,
  `col_ipv4_undef_signed` ipv4 NULL,
  `col_ipv4_undef_signed_not_null` ipv4 NOT NULL,
  `col_ipv6_undef_signed` ipv6 NULL,
  `col_ipv6_undef_signed_not_null` ipv6 NOT NULL,
  `pk` int NULL
) ENGINE=OLAP
UNIQUE KEY(`col_tinyint_undef_signed_not_null`, `col_decimal_5_0__undef_signed_not_null`)
DISTRIBUTED BY HASH(`col_tinyint_undef_signed_not_null`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728",
"enable_mow_light_delete" = "false"
);
    """



    sql """
    DROP TABLE IF EXISTS `table_20_undef_partitions2_keys3_properties4_distributed_by54`;
    """
    sql """

CREATE TABLE `table_20_undef_partitions2_keys3_properties4_distributed_by54` (
  `col_tinyint_undef_signed_not_null` tinyint NOT NULL,
  `col_decimal_5_0__undef_signed_not_null` decimal(5,0) NOT NULL,
  `col_boolean_undef_signed` boolean NULL,
  `col_boolean_undef_signed_not_null` boolean NOT NULL,
  `col_tinyint_undef_signed` tinyint NULL,
  `col_smallint_undef_signed` smallint NULL,
  `col_smallint_undef_signed_not_null` smallint NOT NULL,
  `col_int_undef_signed` int NULL,
  `col_int_undef_signed_not_null` int NOT NULL,
  `col_bigint_undef_signed` bigint NULL,
  `col_bigint_undef_signed_not_null` bigint NOT NULL,
  `col_largeint_undef_signed` largeint NULL,
  `col_largeint_undef_signed_not_null` largeint NOT NULL,
  `col_decimal_5_0__undef_signed` decimal(5,0) NULL,
  `col_decimal_12_2__undef_signed` decimal(12,2) NULL,
  `col_decimal_12_2__undef_signed_not_null` decimal(12,2) NOT NULL,
  `col_decimal_32_6__undef_signed` decimal(32,6) NULL,
  `col_decimal_32_6__undef_signed_not_null` decimal(32,6) NOT NULL,
  `col_date_undef_signed` date NULL,
  `col_date_undef_signed_not_null` date NOT NULL,
  `col_datetime_undef_signed` datetime NULL,
  `col_datetime_undef_signed_not_null` datetime NOT NULL,
  `col_datetime_6__undef_signed` datetime(6) NULL,
  `col_datetime_6__undef_signed_not_null` datetime(6) NOT NULL,
  `col_char_50__undef_signed` char(50) NULL,
  `col_char_50__undef_signed_not_null` char(50) NOT NULL,
  `col_varchar_100__undef_signed` varchar(100) NULL,
  `col_varchar_100__undef_signed_not_null` varchar(100) NOT NULL,
  `col_string_undef_signed` text NULL,
  `col_string_undef_signed_not_null` text NOT NULL,
  `col_ipv4_undef_signed` ipv4 NULL,
  `col_ipv4_undef_signed_not_null` ipv4 NOT NULL,
  `col_ipv6_undef_signed` ipv6 NULL,
  `col_ipv6_undef_signed_not_null` ipv6 NOT NULL,
  `pk` int NULL
) ENGINE=OLAP
DUPLICATE KEY(`col_tinyint_undef_signed_not_null`, `col_decimal_5_0__undef_signed_not_null`)
DISTRIBUTED BY HASH(`pk`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);
    """

    sql """INSERT INTO `table_20_undef_partitions2_keys3_properties4_distributed_by54` VALUES (-5,-3,0,1,16,17,10,147483648,-12,NULL,17,'10','8',300,-6.00,-8.00,-6.000000,100.000000,'2030-12-31','2014-08-12','2024-06-30 12:01:02','2024-02-20 00:28:00','2024-02-20 00:29:00.000000','2024-02-20 00:03:00.000000','B❤️','2030-12-31 23:59:59','C❤️','a❤️','❤️❤️','DEF_suffix','227.74.10.141','192.26.168.254','c27f:5d3:6e23:e4a2:4f61:f804:4557:c907','545c:77a8:5c6a:dcb4:5938:f4f2:7a31:837b',4),(19,-20,0,1,13,10,5,4,-5,0,-2,'-9','8',-3,-20.00,5.00,15.000000,300.000000,'2014-08-12','2023-01-15','2030-12-31 00:00:00','2007-11-22 00:00:00',NULL,'2024-02-20 00:37:00.000000','c❤️','AA','B','dc','AA','BC','33.98.33.241','116.0.200.26','c27f:5d3:6e23:e4a2:4f61:f804:4557:c907','bb00:c853:1028:3e03:8508:b333:c7d8:7bdc',19),(-17,-5,NULL,0,-16,-12,-2,20240803,245,-5,12,'-3','3',19,9.00,0.00,16.000000,10.000000,'2051-01-02','2051-01-02','2024-02-20 00:15:00','2024-02-20 00:11:00','2024-02-20 00:11:00.000000','2024-02-20 00:21:00.000000','BA','bc','❤️b','❤️❤️','b❤️','CC',NULL,'100.42.114.175','ad35:5231:cf77:ab80:b9ee:ab26:7fe4:e163','9aa4:5aea:5e94:1f2d:aae8:ca9b:882b:752',15),(15,-4,1,1,13,-3253,4,1,245,-8,-5,'-9','10',3,-10.00,11.00,-9.000000,-2.000000,'2008-08-12','2030-12-31','2024-02-20 00:13:00','2023-01-15 08:32:59','2024-02-20 00:06:00.000000','2024-02-20 00:18:00.000000','300.343_suffix','0','b','DD','AC','db','87.107.240.72','69.28.220.248','296a:9dee:62d8:fbe9:d0d7:6e7b:9a4:555a','35bf:d297:9d50:4ed:c771:cf7e:396f:3e0f',3),(17,15,NULL,0,-11,19,-12,4,12,-9,20,'2','-15',7,300.00,100.02,0.000000,-18.000000,'2024-03-01','2024-02-29','2024-02-20 00:09:00','2024-02-20 00:19:00','2024-06-30 12:01:02.000000','2024-02-20 00:17:00.000000','DEF','bd','a','300.343','BA','aa',NULL,'137.166.13.180','86ec:8084:5ea:77ca:4d25:d986:e958:f69d','f90d:8a56:e843:604:e0b8:b9b9:bb5a:35d4',6),(-6,17,0,1,4,8,-2,19,-19,-5,32679,'2','19',-20,8.00,3.00,12.000000,15.000000,'2024-02-27','2024-02-27','2024-02-20 00:00:00','2024-02-20 00:24:00','2024-02-20 00:22:00.000000','2024-02-20 00:25:00.000000','BD','❤️D','a❤️','BD','da','ba','214.110.15.163','81.13.156.154','a1b7:afe1:b356:a709:dd3f:fed1:6401:9d40','c27f:5d3:6e23:e4a2:4f61:f804:4557:c907',17),(2,8,1,1,14,-2,5,-14,-19,300,32679,'-11','17',12,0.00,-3.00,16.000000,-20.000000,'2024-08-03','2024-03-05','2030-12-31 00:00:00','2024-02-20 00:29:00','2006-08-22 00:00:00.000000','2024-02-20 00:29:00.000000','AD','1','AC','20240803_suffix','dc','ca','71.1.182.234','105.182.52.167','120f:139:a47:dd0e:35:afd2:22f0:7f24','f7a7:e0e8:847d:9707:59e2:97d3:b886:33aa',18),(2,300,NULL,0,14,245,-13,19,10,3,-13,'-20','9',300,40.12,-5.00,-4.000000,8.000000,'2024-03-10','2050-12-31','2024-02-20 00:26:00','2024-08-03 13:08:30','2024-02-20 00:39:00.000000','2024-02-20 00:35:00.000000','BB','DC','aa','CC','DEF','d','170.71.68.38','38.136.216.49','c27f:5d3:6e23:e4a2:4f61:f804:4557:c907','a412:1d94:7f84:f6bd:182c:44e2:3d23:c06',10),(-14,8,0,0,1,-11,-18,6275082,20,12,1,'-19','15',0,300.34,5.00,-20.000000,7.000000,'2024-03-08','2024-03-23','2024-02-20 00:19:00','2024-02-20 00:21:00','2024-02-20 00:34:00.000000','2024-02-20 00:04:00.000000','2024-08-03 13:08:30','0_suffix','2024-08-03 13:08:30_suffix','300.343_suffix','DD','A','71.1.182.234','146.243.185.229','6bdb:e8c6:9609:26ed:82c7:2a9a:4cb:6c1b','9aa2:f53d:7344:f0b9:c042:4b41:b994:b79b',14),(1,3,NULL,1,-8,2,19,-11,11,2,8,'8','15',40,-18.00,11.00,17.000000,-15.000000,'2024-03-29','2024-08-03','2024-02-20 00:37:00','2010-12-01 15:03:14','2024-02-20 00:13:00.000000','2024-02-20 00:02:00.000000','2024-07-01_suffix','B','a❤️','AB','B','ab','71.1.182.234','164.168.238.175','f4ea:2c70:bbec:ac3d:ed95:d742:a76:8086','c27f:5d3:6e23:e4a2:4f61:f804:4557:c907',8),(11,0,1,0,3,-9,-13,-7,3,10,17,'12','0',40,300.34,100.02,100.020000,11.000000,'2030-12-31','2024-07-01','2007-07-24 08:06:13','2024-06-30 12:01:02','2030-12-31 23:59:59.000000','2024-02-20 00:26:00.000000','b','❤️D','©','DEF','❤️','p','87.107.240.72','87.107.240.72','f7a7:e0e8:847d:9707:59e2:97d3:b886:33aa','e998:2a8b:f716:e866:f931:87b7:4a14:609d',16),(-18,-7,0,1,-17,-8,-22637,-10,2316504,-14,245,'3','2',NULL,-16.00,19.00,-2.000000,12.000000,'2010-01-02','2024-03-10','2024-02-20 00:05:00','2024-02-20 00:28:00','2024-02-20 00:15:00.000000','2030-12-31 23:59:59.000000','AC','AC','c❤️','CB','dd','AD',NULL,'71.1.182.234','2583:72ea:4013:9085:b600:43bf:9f3f:d853','51cf:6b25:7704:4f43:71d4:700f:828a:6880',11),(6,3,1,0,-4,4,-20,-16,-3,-18,7,'16','9',19,-18.00,-8.00,3.000000,20.000000,'2024-03-29','2051-01-02','2024-02-20 00:18:00','2024-02-20 00:21:00','2024-02-20 00:36:00.000000','2030-12-31 00:00:00.000000','abc','b❤️','dc','20240803','da','A','100.14.67.173','3.227.59.30','f7a7:e0e8:847d:9707:59e2:97d3:b886:33aa','f7a7:e0e8:847d:9707:59e2:97d3:b886:33aa',13),(8,11,NULL,1,-20,NULL,-12,-2,20,-7,32679,'8','-8',1,19.00,-12.00,-3.000000,40.123000,'2003-08-22','2024-03-08','2024-02-20 00:04:00','2024-02-20 00:20:00','2024-02-20 00:04:00.000000','2024-02-20 00:03:00.000000','ba','DA','db','bc','ab','d',NULL,'231.167.46.32',NULL,'c27f:5d3:6e23:e4a2:4f61:f804:4557:c907',2),(-13,-18,1,0,-2,17,-16,18,17,9,2,'-12','-9',-12,19.00,-19.00,-13.000000,10.000000,'2024-02-29','2024-07-01','2024-02-20 00:39:00','2024-02-20 00:32:00','2023-01-15 08:32:59.000000','2014-08-12 00:00:00.000000','cb','2024-07-01','2024-07-01_suffix','da','❤️d','20240803_suffix','170.208.238.200','111.137.75.242','347:90cc:5cca:3630:c140:4bf1:d1d:f755','f7a7:e0e8:847d:9707:59e2:97d3:b886:33aa',12),(-20,-11,0,1,7,-3,6,0,0,-10,-8,'-7','6',0,-3.00,40.12,15.000000,40.123000,'2024-08-03','2024-02-22','2030-12-31 00:00:00','2024-02-20 00:36:00','2017-09-04 00:00:00.000000','2024-02-20 00:15:00.000000','BB','DEF','DEF','BC','300.343','AA','99.231.26.247','155.120.229.118','6718:cffc:ee1d:ba48:dcbe:d07b:c61a:5952','c27f:5d3:6e23:e4a2:4f61:f804:4557:c907',5),(-12,-10,NULL,0,5,-1,-10,17528,6,-7142,2,'19','8',9,9.00,40.00,-7.000000,6.000000,'2024-03-07','2024-03-25','2024-02-20 00:33:00','2024-02-20 00:15:00','2024-02-20 00:09:00.000000','2024-02-20 00:25:00.000000','2030-12-31 23:59:59_suffix','❤️❤️','DEF_suffix','🍕','AB','2024-08-03 13:08:30',NULL,'200.61.206.150','f7a7:e0e8:847d:9707:59e2:97d3:b886:33aa','c27f:5d3:6e23:e4a2:4f61:f804:4557:c907',1),(-2,-1,0,0,1,-7,20,13,1096531555,-8,7,'-18','-5',-1,-19.00,-4.00,NULL,1.000000,'2024-03-26','2024-02-20','2024-02-20 00:07:00','2024-02-20 00:19:00','2023-01-15 08:32:59.000000','2024-02-20 00:01:00.000000','ca','DA','2024-08-03 13:08:30_suffix','❤️C','abc_suffix','c','71.1.182.234','68.190.76.113','514a:7f18:75f0:5984:1f74:508a:db45:5668','5db9:3d27:a9fc:8b5b:cfcd:43e6:1a14:6b50',7),(-15,-12,NULL,0,0,3,12,5,4,NULL,20,'-16','-5',14,-15.00,300.34,-17.000000,8.000000,'2030-12-31','2024-03-09','2024-02-20 00:04:00','2024-08-03 13:08:30','2024-02-20 00:34:00.000000','2024-07-01 00:00:00.000000','❤️❤️','1','bc','ABC','b','abc_suffix','3.100.77.46','217.15.171.141','7528:e2ba:7a5d:9aa5:5f91:f9c1:8aea:44b7','1f9e:f381:496b:f253:9b14:22b7:1f16:2701',9),(-6,18,NULL,0,-19,-8,13,-6,-13,20240803,147483648,'8','18',NULL,17.00,18.00,300.000000,9.000000,'2024-03-17','2024-03-08','2024-02-20 00:31:00','2024-02-20 00:07:00','2024-02-20 00:32:00.000000','2024-02-20 00:12:00.000000','A❤️','0_suffix','❤️d','C❤️','DA','CB','114.156.34.160','71.1.182.234','f7a7:e0e8:847d:9707:59e2:97d3:b886:33aa','57ef:ddf5:42c0:e95e:d219:13d9:e54b:b554',0);
    """

    // load and execute data.sql in the same directory, use simple direct load as in other test cases
    sql new File("${context.config.dataPath}/query_p0/set/rqg_prod_20240226/data").text
    sql 'sync'

    qt_test """
    select t1.col_string_undef_signed from table_200_undef_partitions2_keys3_properties4_distributed_by511 as t1 where t1. col_datetime_6__undef_signed_not_null <= '2009-07-21 12:26:28' except distinct  select SUBSTRING(t1.col_varchar_100__undef_signed_not_null, 1, -9) from table_20_undef_partitions2_keys3_properties4_distributed_by54 as t1 where t1. col_char_50__undef_signed >= 'v' order by 1;
    """
}
