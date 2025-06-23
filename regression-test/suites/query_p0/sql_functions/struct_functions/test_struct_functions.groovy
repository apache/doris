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

suite("test_struct_functions") {
    def tableName = "tbl_test_struct_functions"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` INT(11) NULL,
              `k2` STRUCT<f1:TINYINT,f2:SMALLINT,f3:INT,f4:BIGINT,f5:LARGEINT> NULL,
              `k3` STRUCT<f1:FLOAT,f2:DOUBLE,f3:DECIMAL(10,3)> NULL,
              `k4` STRUCT<f1:DATE,f2:DATETIME,f3:DATEV2,f4:DATETIMEV2> NULL,
              `k5` STRUCT<f1:CHAR(10),f2:VARCHAR(10),f3:STRING> NOT NULL
            )
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1,{1,2,3,4,5},{1.0,3.33,0.001},{"2023-04-01","2023-04-01 12:00:00","2023-04-01","2023-04-01 12:00:00.999"},{'a','abc','abc'}) """
    sql """ INSERT INTO ${tableName} VALUES(2,struct(1,1000,10000000,100000000000,100000000000),struct(1.0,2.143,0.001),struct("2023-04-01","2023-04-01 12:00:00","2023-04-01","2023-04-01 12:00:00.999"),struct("hi","doris","hello doris")) """
    sql """ INSERT INTO ${tableName} VALUES(3,named_struct("f1",5,"f2",4,"f3",3,"f4",2,"f5",1),named_struct("f1",2.3,"f2",23.3,"f3",0.333),named_struct('f1','2023-04-01','f2','2023-04-01 12:00:00','f3','2023-04-01','f4','2023-04-01 12:00:00.999'),named_struct('f1','a','f2','abc','f3','abc')) """
    sql """ INSERT INTO ${tableName} VALUES(4,struct(1,NULL,3,NULL,5),{2.0,NULL,0.001},{'2023-04-01',NULL,'2023-04-01',NULL},struct('a',NULL,'abc')) """
    sql """ INSERT INTO ${tableName} VALUES(5,NULL,NULL,NULL,{NULL, NULL, NULL}) """
    sql """ INSERT INTO ${tableName} VALUES(6,NULL,NULL,NULL,{"NULL",'null',NULL}) """

    qt_select_all "SELECT * FROM ${tableName} ORDER BY k1"

    qt_select_struct_element_1 "SELECT struct_element(k2,'f1'),struct_element(k2,'f2'),struct_element(k2,'f3'),struct_element(k2,'f4'),struct_element(k2,'f5') FROM ${tableName} ORDER BY k1"
    qt_select_struct_element_2 "SELECT struct_element(k3,'f1'),struct_element(k3,'f2'),struct_element(k3,'f3') FROM ${tableName} ORDER BY k1"
    qt_select_struct_element_3 "SELECT struct_element(k4,1),struct_element(k4,2),struct_element(k4,3),struct_element(k4,4) FROM ${tableName} ORDER BY k1"
    qt_select_struct_element_4 "SELECT struct_element(k5,1),struct_element(k5,2),struct_element(k5,3) FROM ${tableName} ORDER BY k1"

    //The precision of the decimal type in the test select is inconsistent with the precision of the function named_struct containing the decimal type.
    sql """ drop table if exists t01 --force """;
    sql """ create table if not exists t01 (a decimal(6,3), d struct<col:bigint, col1:decimal(7,2)>) properties ("replication_num"="1");"""
    sql """ insert into t01 values (123.321, named_struct('col', 1, 'col1', 345.24));"""
    qt_sql_before """ select named_struct("col_11", a, "col_12", d) from t01; """
    sql """ insert into t01 values (123.331, named_struct('col', 1, 'col1', 12345.24));"""
    qt_sql_after """ select named_struct("col_11", a, "col_12", d) from t01 order by a; """
}
