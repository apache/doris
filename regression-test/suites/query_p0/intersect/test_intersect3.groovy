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

suite("test_intersect3") {
    sql """ DROP TABLE IF EXISTS `table_500_undef_partitions2_keys3_properties4_distributed_by53` """
    sql """ 
        create table table_500_undef_partitions2_keys3_properties4_distributed_by53 (
        col_bigint_undef_signed_not_null bigint  not null  ,
        col_char_25__undef_signed_not_null char(25)  not null  ,
        col_boolean_undef_signed boolean  null  ,
        col_boolean_undef_signed_not_null boolean  not null  ,
        col_tinyint_undef_signed tinyint  null  ,
        col_tinyint_undef_signed_not_null tinyint  not null  ,
        col_smallint_undef_signed smallint  null  ,
        col_smallint_undef_signed_not_null smallint  not null  ,
        col_int_undef_signed int  null  ,
        col_int_undef_signed_not_null int  not null  ,
        col_bigint_undef_signed bigint  null  ,
        col_float_undef_signed float  null  ,
        col_float_undef_signed_not_null float  not null  ,
        col_double_undef_signed double  null  ,
        col_double_undef_signed_not_null double  not null  ,
        col_decimal_5_0__undef_signed decimal(5,0)  null  ,
        col_decimal_5_0__undef_signed_not_null decimal(5,0)  not null  ,
        col_decimal_12_2__undef_signed decimal(12,2)  null  ,
        col_decimal_12_2__undef_signed_not_null decimal(12,2)  not null  ,
        col_decimal_32_6__undef_signed decimal(32,6)  null  ,
        col_decimal_32_6__undef_signed_not_null decimal(32,6)  not null  ,
        col_date_undef_signed date  null  ,
        col_date_undef_signed_not_null date  not null  ,
        col_datetime_undef_signed datetime  null  ,
        col_datetime_undef_signed_not_null datetime  not null  ,
        col_datetime_3__undef_signed datetime(3)  null  ,
        col_datetime_3__undef_signed_not_null datetime(3)  not null  ,
        col_char_25__undef_signed char(25)  null  ,
        col_varchar_25__undef_signed varchar(25)  null  ,
        col_varchar_25__undef_signed_not_null varchar(25)  not null  ,
        col_varchar_100__undef_signed varchar(100)  null  ,
        col_varchar_100__undef_signed_not_null varchar(100)  not null  ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_bigint_undef_signed_not_null, col_char_25__undef_signed_not_null)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """ 
        insert into table_500_undef_partitions2_keys3_properties4_distributed_by53(pk,col_boolean_undef_signed,col_boolean_undef_signed_not_null,col_tinyint_undef_signed,col_tinyint_undef_signed_not_null,col_smallint_undef_signed,col_smallint_undef_signed_not_null,col_int_undef_signed,col_int_undef_signed_not_null,col_bigint_undef_signed,col_bigint_undef_signed_not_null,col_float_undef_signed,col_float_undef_signed_not_null,col_double_undef_signed,col_double_undef_signed_not_null,col_decimal_5_0__undef_signed,col_decimal_5_0__undef_signed_not_null,col_decimal_12_2__undef_signed,col_decimal_12_2__undef_signed_not_null,col_decimal_32_6__undef_signed,col_decimal_32_6__undef_signed_not_null,col_date_undef_signed,col_date_undef_signed_not_null,col_datetime_undef_signed,col_datetime_undef_signed_not_null,col_datetime_3__undef_signed,col_datetime_3__undef_signed_not_null,col_char_25__undef_signed,col_char_25__undef_signed_not_null,col_varchar_25__undef_signed,col_varchar_25__undef_signed_not_null,col_varchar_100__undef_signed,col_varchar_100__undef_signed_not_null) values (0,null,false,-128,127,902,-23671,-2147483648,721920335,5,16427,94.4178,300.343,77.30092439650055,40.123,41.1140,50.1399,300.343,40.123,40.123,24.0569,'2024-08-03 13:08:30','9999-12-31','2010-10-19','2024-06-30 12:01:02','2014-08-12','2010-12-15 15:29:30','no','had','x','y','9999-12-31 23:59:59','2024-07-01'),(1,false,true,127,-25,-25700,-25007,1436660686,-2147483648,20240803,1,51.070793,68.587036,5.517655576994014,92.33623919107137,29.0609,67.0897,44.0258,1.0477,300.343,9.0135,'9999-12-31','9999-12-31','2024-08-03 13:08:30','9999-12-31','9999-12-31 23:59:59','2014-08-12','2024-07-01','9999-12-31 23:59:59','2024-07-01','d','1','300.343'),(2,true,true,121,3,25035,11279,0,0,3093870,32679,22.470251,100.02,40.123,27.50871095685667,40.123,100.02,300.343,30.1546,null,100.02,'9999-12-31','2024-06-30 12:01:02','2011-12-05','2014-08-12','2010-11-01 18:32:19','2017-05-07','0','1','she','d','from','do'),(3,false,true,-128,-128,0,-32768,533470644,1378292201,147483648,6,null,100.02,null,37.46375213353008,100.02,64.1230,53.1196,40.123,100.02,2.1520,'2024-06-30 12:01:02','2019-09-05','2014-08-12','2024-06-30 12:01:02','9999-12-31','2005-02-14 01:53:53','so','n','2024-08-03 13:08:30','are','k','9999-12-31 23:59:59'),(4,null,false,null,14,19937,-27188,-1263819945,2147483647,1,300.343,50.326797,40.123,300.343,92.24993447247348,null,18.1494,69.1981,100.02,79.1568,83.1367,'2024-07-01','9999-12-31 23:59:59','2023-01-15 08:32:59','2004-01-02','9999-12-31','2010-11-27 04:41:56','2024-08-03 13:08:30','0',null,'will','tell','as'),(5,false,false,8,127,-1657,0,-2147483648,-1306478472,300.343,-20449,40.123,300.343,91.6174997855454,93.37335999679732,48.0750,4.1226,100.02,24.0800,72.1787,69.1348,'2023-01-15 08:32:59','2011-06-17','9999-12-31','9999-12-31','2018-03-22 14:49:31','2008-04-14 21:35:10','0','me','20240803','v','2024-08-03 13:08:30','2024-08-03 13:08:30'),(6,false,false,1,5,11575,32767,-1444959794,0,-7595114,300.343,40.123,40.123,100.02,100.02,87.0137,100.02,38.0365,40.123,56.1037,40.123,'2024-07-01','2023-01-15 08:32:59','2002-06-17','2024-07-01',null,'2003-04-17','2024-07-01','but','hey','0','1','20240803');
    """
    qt_select1 """ 
        select col_decimal_12_2__undef_signed_not_null, col_tinyint_undef_signed_not_null from table_500_undef_partitions2_keys3_properties4_distributed_by53 intersect select col_decimal_12_2__undef_signed_not_null, col_int_undef_signed from table_500_undef_partitions2_keys3_properties4_distributed_by53;
    """
}
