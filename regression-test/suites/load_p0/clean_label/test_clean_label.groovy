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

suite("test_clean_label") {
    // define a sql table
    def testTable = "tbl_test_clean_label"
    def dbName = context.config.getDbNameByFile(context.file)
    
    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL COMMENT "",
              `k2` INT(11) NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
            """
        
        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
    }

    // case1: 
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        create_test_table.call(testTable)

        test {
            sql "insert into ${testTable} with label clean_label_test1 select 1, 2;"
        }

        test {
            sql "insert into ${testTable} with label clean_label_test2 select 1, 2;"
        }

        test {
            sql "insert into ${testTable} with label clean_label_test3 select 1, 2;"
        }

        test {
            sql "insert into ${testTable} with label clean_label_test4 select 1, 2;"
        }

        qt_select "select * from ${testTable} order by k1"

        test {
            sql "insert into ${testTable} with label clean_label_test4 select 1, 2;"
            exception "errCode = 2, detailMessage = Label"
        }

        test {
            sql "clean label clean_label_test4 from ${dbName};"
        }

        test {
            sql "insert into ${testTable} with label clean_label_test4 select 1, 2;"
        }

        test {
            sql "insert into ${testTable} with label clean_label_test1 select 1, 2;"
            exception "errCode = 2, detailMessage = Label"
        }

        test {
            sql "clean label from ${dbName};"
        }

        test {
            sql "insert into ${testTable} with label clean_label_test1 select 1, 2;"
        }

        test {
            sql "insert into ${testTable} with label clean_label_test2 select 1, 2;"
        }

        test {
            sql "insert into ${testTable} with label clean_label_test3 select 1, 2;"
        }

        test {
            sql "insert into ${testTable} with label clean_label_test4 select 1, 2;"
        }

        qt_select "select * from ${testTable} order by k1;"

        test {
            sql "clean label from ${dbName};"
        }

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}
