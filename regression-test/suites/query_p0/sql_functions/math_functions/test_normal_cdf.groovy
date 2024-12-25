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

suite("test_normal_cdf") {
    def dbName = "test_normal_cdf"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE $dbName"


    
    sql """DROP TABLE IF EXISTS `tb`"""
    sql """ CREATE TABLE `tb` (
        `id` int ,

        `k1` double ,
        `k2` double ,
        `k3` double ,

        `k11` double not NULL,
        `k22` double not NULL,
        `k33` double not NULL
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 5 properties("replication_num" = "1");
        """



    sql """ insert into `tb` values( 1,     0, 1, 1.96,          0, 1, 1.96 ) """  // 0.9750021048517796
    sql """ insert into `tb` values( 2,     10, 9, 10,          10, 9, 10 ) """  // 0.5
    sql """ insert into `tb` values( 3,     -1.5, 2.1, -7.8,    -1.5, 2.1, -7.8) """  // 0.0013498980316301035
    
    sql """ insert into `tb` values( 4,     0 , 0 , 1,          0 , 0 , 1 ) """  // NULL
    sql """ insert into `tb` values( 5,     0 , -1 , 1,         0 , -1 , 1 ) """  // NULL


    sql """ insert into `tb` values( 6,     NULL, NULL, NULL,          0, 1, 1.96 ) """  // 0.9750021048517796
    sql """ insert into `tb` values( 7,     0, NULL, NULL,          0, 1, 1.96 ) """  // 0.9750021048517796
    sql """ insert into `tb` values( 8,     0, 1 , NULL,          0, 1, 1.96 ) """  // 0.9750021048517796


    sql """ insert into `tb` values( 9,      0, NULL, 1.96,          0, 1, 1.96 ) """  // 0.9750021048517796
    sql """ insert into `tb` values( 10,     0, NULL, NULL,          0, 1, 1.96 ) """  // 0.9750021048517796
    sql """ insert into `tb` values( 11,     0, NULL , 1.96,          0, 1, 1.96 ) """  // 0.9750021048517796


    sql """ insert into `tb` values( 12,     NULL, 1, 1.96,          0, 1, 1.96 ) """  // 0.9750021048517796
    sql """ insert into `tb` values( 13,     NULL, 1, NULL,          0, 1, 1.96 ) """  // 0.9750021048517796
    sql """ insert into `tb` values( 14,     NULL, 1 , 1.96,          0, 1, 1.96 ) """  // 0.9750021048517796


    qt_test_1 """ select normal_cdf(k1,k2,k3),normal_cdf(k11,k22,k33)  from tb order by id """  

    qt_test_2 """ select normal_cdf(k1,k2,1.96),normal_cdf(k11,k22,1.96)  from tb order by id """  
    qt_test_3 """ select normal_cdf(0,k2,k3),normal_cdf(0,k22,k33)  from tb order by id """  
    qt_test_4 """ select normal_cdf(k1,1,k3),normal_cdf(k11,1,k33)  from tb order by id """  

    qt_test_5 """ select normal_cdf(0,1,k3),normal_cdf(0,1,k33)  from tb order by id """  
    qt_test_6 """ select normal_cdf(k1,1,1.96),normal_cdf(k11,1,1.96)  from tb order by id """  
    qt_test_7 """ select normal_cdf(0,k2,1.96),normal_cdf(0,k2,1.96)  from tb order by id """  

    qt_test_8 """ select normal_cdf(k1,k2,NULL),normal_cdf(k11,k22,NULL)  from tb order by id """  
    qt_test_9 """ select normal_cdf(NULL,k2,k3),normal_cdf(NULL,k22,k33)  from tb order by id """  
    qt_test_10 """ select normal_cdf(k1,NULL,k3),normal_cdf(k1,NULL,k33)  from tb order by id """  

    qt_test_11 """ select normal_cdf(nullable(k1),NULL,k3),normal_cdf(nullable(0),NULL,k33)  from tb order by id """  

    qt_test_12 """ select id,k1,k2,normal_cdf(0,1,1.96),normal_cdf(k1,k2,1.96),normal_cdf(k11,k22,1.96)  from tb where id =1 ;"""

    qt_test_13 """ select normal_cdf( 0, 1, 1.96 ) ; """ 
    qt_test_14 """ select normal_cdf( nullable(0), 1, 1.96 ) ; """ 
    qt_test_15 """ select normal_cdf( nullable(0), nullable(1), 1.96 ) ; """ 
    qt_test_16 """ select normal_cdf( nullable(0), NULL  , 1.96 ) ; """     
    qt_test_17 """ select normal_cdf( nullable(0), NULL  , 1.96 ) ; """
    qt_test_18 """ select normal_cdf( 0, 1  , NULL ) ; """
    qt_test_19 """ select normal_cdf( 0, -1,1 ) ; """


}
