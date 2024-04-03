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

suite("test_struct_insert") {
    // define a sql table
    def testTable = "tbl_test_struct_insert"

    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL,
              `k2` STRUCT<f1:BOOLEAN,f2:TINYINT,f3:SMALLINT,f4:INT,f5:INT,f6:BIGINT,f7:LARGEINT> NULL,
              `k3` STRUCT<f1:FLOAT,f2:DOUBLE,f3:DECIMAL(10,3)> NULL,
              `k4` STRUCT<f1:DATE,f2:DATETIME,f3:DATEV2,f4:DATETIMEV2> NULL,
              `k5` STRUCT<f1:CHAR(10),f2:VARCHAR(10),f3:STRING> NOT NULL
            )
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
            """

        // DDL/DML return 1 row and 5 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
    }


    sql "DROP TABLE IF EXISTS ${testTable}"
    create_test_table.call(testTable)

    sql "set enable_insert_strict = true"

    // TODO reopen these cases after we could process cast right in BE and FE
    //  current, it is do right thing in a wrong way. because cast varchar in struct is wrong
    // invalid cases
    // test {
    //     // k5 is not nullable, can not insert null
    //     sql "insert into ${testTable} values (111,null,null,null,null)"
    //     exception "Insert has filtered data"
    // }
    // test {
    //     // size of char type in struct is 10, can not insert string with length more than 10
    //     sql "insert into ${testTable} values (112,null,null,null,{'1234567890123',null,null})"
    //     exception "Insert has filtered data"
    // }
    // test {
    //     // size of varchar type in struct is 10, can not insert string with length more than 10
    //     sql "insert into ${testTable} values (113,null,null,null,{null,'12345678901234',null})"
    //     exception "Insert has filtered data"
    // }
    // normal cases include nullable and nullable nested fields
    sql "INSERT INTO ${testTable} VALUES(1, {1,11,111,1111,11111,11111,111111},null,null,{'','',''})"
    sql "INSERT INTO ${testTable} VALUES(2, {null,null,null,null,null,null,null},{2.1,2.22,2.333},null,{null,null,null})"
    sql "INSERT INTO ${testTable} VALUES(3, null,{null,null,null},{'2023-02-23','2023-02-23 00:10:19','2023-02-23','2023-02-23 00:10:19'},{'','',''})"
    sql "INSERT INTO ${testTable} VALUES(4, null,null,{null,null,null,null},{'abc','def','hij'})"

    // select the table and check whether the data is correct
    qt_select "select * from ${testTable} order by k1"
}