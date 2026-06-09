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

    qt_select_struct_element_1 "SELECT element_at(k2,'f1'),element_at(k2,'f2'),element_at(k2,'f3'),element_at(k2,'f4'),element_at(k2,'f5') FROM ${tableName} ORDER BY k1"
    qt_select_struct_element_2 "SELECT element_at(k3,'f1'),element_at(k3,'f2'),element_at(k3,'f3') FROM ${tableName} ORDER BY k1"
    qt_select_struct_element_3 "SELECT element_at(k4,1),element_at(k4,2),element_at(k4,3),element_at(k4,4) FROM ${tableName} ORDER BY k1"
    qt_select_struct_element_4 "SELECT element_at(k5,1),element_at(k5,2),element_at(k5,3) FROM ${tableName} ORDER BY k1"

    // DORIS-26105: subscript syntax `s['field']` / `s[index]` on STRUCT should work the same as
    // element_at / struct_element and must not crash the BE (especially for NULL struct rows).
    qt_select_struct_subscript_1 "SELECT k2['f1'],k2['f2'],k2['f3'],k2['f4'],k2['f5'] FROM ${tableName} ORDER BY k1"
    qt_select_struct_subscript_2 "SELECT k3['f1'],k3['f2'],k3['f3'] FROM ${tableName} ORDER BY k1"
    qt_select_struct_subscript_3 "SELECT k4[1],k4[2],k4[3],k4[4] FROM ${tableName} ORDER BY k1"
    qt_select_struct_subscript_4 "SELECT k5[1],k5[2],k5[3] FROM ${tableName} ORDER BY k1"

    // exact repro from DORIS-26105: subscript on a struct column whose value is NULL
    sql """ DROP TABLE IF EXISTS tbl_struct_subscript_null """
    sql """
            CREATE TABLE IF NOT EXISTS tbl_struct_subscript_null (
              id int,
              profile STRUCT<flag:BOOLEAN,score:INT> NULL
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
    sql """ INSERT INTO tbl_struct_subscript_null VALUES(1, null) """
    qt_select_struct_subscript_null "SELECT profile['score'] FROM tbl_struct_subscript_null"

    //The precision of the decimal type in the test select is inconsistent with the precision of the function named_struct containing the decimal type.
    sql """ drop table if exists t01 --force """;
    sql """ create table if not exists t01 (a decimal(6,3), d struct<col:bigint, col1:decimal(7,2)>) properties ("replication_num"="1");"""
    sql """ insert into t01 values (123.321, named_struct('col', 1, 'col1', 345.24));"""
    qt_sql_before """ select named_struct("col_11", a, "col_12", d) from t01; """
    sql """ insert into t01 values (123.331, named_struct('col', 1, 'col1', 12345.24));"""
    qt_sql_after """ select named_struct("col_11", a, "col_12", d) from t01 order by a; """
}
