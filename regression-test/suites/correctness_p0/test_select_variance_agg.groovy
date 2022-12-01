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

 suite("test_select_variance_agg") {
     def tableName = "test_variance"


     sql """ DROP TABLE IF EXISTS ${tableName} """
     sql """
         CREATE TABLE IF NOT EXISTS ${tableName} ( `aa` int NULL COMMENT "", `bb` decimal(27,9), `decimal32_col` decimalv3(5,2), `decimal64_col` decimalv3(15,9), `decimal128_col` decimal(27,9) NULL COMMENT "" )
         ENGINE=OLAP UNIQUE KEY(`aa`) DISTRIBUTED BY HASH(`aa`) BUCKETS 3 
         PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "in_memory" = "false", "storage_format" = "V2" );
     """

     sql """ INSERT INTO ${tableName} VALUES 
         (123,34, 1.22,6666.6666,34),
         (423,78,2.33,777777.7777,78),
         (3,23,4.34,2222.2222,23); """

     // test_vectorized
     sql """ set enable_vectorized_engine = true; """

     qt_select_default """  select variance(aa) from ${tableName}; """

     // doris decimal variance implementation have deviation,
     // use round to check result
     qt_select_default2 """ select round(variance(bb), 6) from ${tableName}; """
     qt_select_decimal32 """ select round(variance(decimal32_col), 6) from ${tableName}; """
     qt_select_decimal64 """ select round(variance(decimal64_col), 6) from ${tableName}; """
     qt_select_decimal128 """ select round(variance(decimal128_col), 6) from ${tableName}; """

 }