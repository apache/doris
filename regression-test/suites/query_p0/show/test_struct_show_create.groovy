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

suite("test_struct_show_create", "query") {
    // define a sql table
    def testTable = "test_struct_show_create"


    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL COMMENT "",
              `k2` STRUCT<f1:SMALLINT> NOT NULL COMMENT "",
              `k3` STRUCT<f1:SMALLINT, f2:INT(11)> NOT NULL COMMENT "",
              `k4` STRUCT<f1:SMALLINT, f2:INT(11), f3:BIGINT> NOT NULL COMMENT "",
              `k5` STRUCT<f1:SMALLINT, f2:INT(11), f3:BIGINT, f4:CHAR> NOT NULL COMMENT "",
              `k6` STRUCT<f1:SMALLINT, f2:INT(11), f3:BIGINT, f4:CHAR, f5:VARCHAR(20)> NULL COMMENT "",
              `k7` STRUCT<f1:SMALLINT, f2:INT(11), f3:BIGINT, f4:CHAR, f5:VARCHAR(20), f6:DATE> NOT NULL COMMENT "",
              `k8` STRUCT<f1:SMALLINT, f2:INT(11), f3:BIGINT, f4:CHAR, f5:VARCHAR(20), f6:DATE, f7:DATETIME> NOT NULL COMMENT "",
              `k9` STRUCT<f1:SMALLINT, f2:INT(11), f3:BIGINT, f4:CHAR, f5:VARCHAR(20), f6:DATE, f7:DATETIME, f8:FLOAT> NOT NULL COMMENT "",
              `k10` STRUCT<f1:SMALLINT, f2:INT(11), f3:BIGINT, f4:CHAR, f5:VARCHAR(20), f6:DATE, f7:DATETIME, f8:FLOAT, f9:DOUBLE> NOT NULL COMMENT "",
              `k11` STRUCT<f1:SMALLINT, f2:INT(11), f3:BIGINT, f4:CHAR, f5:VARCHAR(20), f6:DATE, f7:DATETIME, f8:FLOAT, f9:DOUBLE, f10:DECIMAL(20, 6)> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            );
            """

        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        // insert 1 row to check whether the table is ok
        def result2 = sql """ INSERT INTO ${testTable} VALUES
                            (100, 
                             {128}, 
                             {128, 32768},
                             {128, 32768, 2147483648},
                             {128, 32768, 2147483648, 'c'},
                             {128, 32768, 2147483648, 'c', "doris"},
                             {128, 32768, 2147483648, 'c', "doris", '2023-02-10'},
                             {128, 32768, 2147483648, 'c', "doris", '2023-02-10', '2023-02-10 12:30:00'},
                             {128, 32768, 2147483648, 'c', "doris", '2023-02-10', '2023-02-10 12:30:00', 0.67},
                             {128, 32768, 2147483648, 'c', "doris", '2023-02-10', '2023-02-10 12:30:00', 0.67, 0.878787878}, 
                             {128, 32768, 2147483648, 'c', "doris", '2023-02-10', '2023-02-10 12:30:00', 0.67, 0.878787878, 6.67})
                            """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        create_test_table.call(testTable)
        qt_select_count "select count(k2), count(k3), count(k4), count(k5), count(6), count(k7), count(k8), count(k9), count(k10), count(11) from ${testTable}"
        def res = sql "show create table ${testTable}"
        assertTrue(res.size() != 0)
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}
