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
    sql """ADMIN SET FRONTEND CONFIG('enable_struct_type'='true')"""
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` INT(11) NULL,
              `k2` STRUCT<f1:TINYINT,f2:SMALLINT,f3:INT,f4:BIGINT,f5:LARGEINT> NULL,
              `k3` STRUCT<f1:FLOAT,f2:DOUBLE,f3:DECIMAL(3,3)> NULL,
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
    sql """ INSERT INTO ${tableName} VALUES(1,{1,2,3,4,5},{1.0,3.33,1.001},{"2023-04-01","2023-04-01 12:00:00","2023-04-01","2023-04-01 12:00:00.999"},{'a','abc','abc'}) """
    sql """ INSERT INTO ${tableName} VALUES(2,struct(1,1000,10000000,100000000000,100000000000),struct(1.0,2.143,1.001),struct("2023-04-01","2023-04-01 12:00:00","2023-04-01","2023-04-01 12:00:00.999"),struct("hi","doris","hello doris")) """
    sql """ INSERT INTO ${tableName} VALUES(3,named_struct("f1",5,"f2",4,"f3",3,"f4",2,"f5",1),named_struct("f1",2.3,"f2",23.3,"f3",2.333),named_struct('f1','2023-04-01','f2','2023-04-01 12:00:00','f3','2023-04-01','f4','2023-04-01 12:00:00.999'),named_struct('f1','a','f2','abc','f3','abc')) """
    sql """ INSERT INTO ${tableName} VALUES(4,struct(1,NULL,3,NULL,5),{2.0,NULL,2.000},{'2023-04-01',NULL,'2023-04-01',NULL},struct('a',NULL,'abc')) """
    sql """ INSERT INTO ${tableName} VALUES(5,NULL,NULL,NULL,{NULL, NULL, NULL}) """
    sql """ INSERT INTO ${tableName} VALUES(6,NULL,NULL,NULL,{"NULL",'null',NULL}) """

    qt_select_all "SELECT * FROM ${tableName} ORDER BY k1"
}
