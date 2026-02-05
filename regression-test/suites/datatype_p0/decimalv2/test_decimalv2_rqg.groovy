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

suite("test_decimalv2_rqg") {


    sql """ADMIN SET FRONTEND CONFIG ('disable_decimalv2' = 'false')"""

    sql """
        CREATE TABLE IF NOT EXISTS TEMPDATA(id INT, data INT) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """
    sql """
        INSERT INTO TEMPDATA values(1, 1);
    """

    sql """CREATE TABLE IF NOT EXISTS DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE(id INT, data DECIMALV2(10,0)  NOT NULL) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');"""

    sql """
    INSERT INTO DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE values (0, 1234567890);"""
    sql """
    INSERT INTO DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE values (1, 9999999999);"""
    sql """
    INSERT INTO DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE values (2, 1000000000);"""
    sql """
    INSERT INTO DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE values (3, 1111111111);"""
    sql """
    INSERT INTO DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE values (4, -1234567890);"""
    sql """
    INSERT INTO DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE values (5, -9999999999);"""
    sql """
    INSERT INTO DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE values (6, -1000000000);"""
    sql """
    INSERT INTO DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE values (7, -1111111111);"""
    sql """
    INSERT INTO DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE values (8, 1);"""
    sql """
    INSERT INTO DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE values (9, 0);"""
    sql """
    INSERT INTO DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE values (10, -1);"""


    sql """
        SELECT CEIL(ARG0,CAST(-1 AS INT)) FROM (SELECT TEMPDATA . data, TABLE0.ARG0 FROM TEMPDATA CROSS JOIN (SELECT data AS ARG0 FROM DECIMALV2_10_0_DATA_NOT_EMPTY_NOT_NULLABLE ) AS TABLE0) t ;
    """
}