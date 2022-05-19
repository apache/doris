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

suite("global_dict") {
    def tableCount = 5

    sql """ set enable_vectorized_engine=true """

    //create tables with 10 low cardinality columns each.
    for (int i = 0; i < tableCount; ++i) {
        def tableName = """dict_table_${i}"""
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    col_0 int,
                    col_1 string,
                    col_2 varchar(30),
                    col_3 char(20),
                    col_4 string,
                    col_5 varchar(31),
                    col_6 char(21),
                    col_7 string,
                    col_8 varchar(32),
                    col_9 char(22),
                    col_10 string
                )
                DUPLICATE KEY(col_0)
                DISTRIBUTED BY HASH(col_0) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1"
                )
            """
        for (int j = 1; j <= 10; ++j) {
            def colName = """col_${j}"""
            sql """ ALTER TABLE ${tableName} MODIFY COLUMN ${colName} LOW_CARDINALITY TRUE """
        }
        for (int n = 0; n < 20; ++n) {
            def tmp = n % 10
            def strVal = """'value_${tmp}'"""
            sql """ INSERT INTO ${tableName} VALUES( 0, ${strVal}, ${strVal}, ${strVal}, ${strVal}, ${strVal}, 
                                                        ${strVal}, ${strVal}, ${strVal}, ${strVal}, ${strVal} ) """
        }
        if (i == 0) {
            //make sure dict_table_0's global dict info is generated.
            sleep(20000)
        }
    }

    try {
        // only verify dict_table_0's global dict info is correct
        for (int j = 1; j <= 10; ++j) {
            def colName = """col_${j}"""
            explain {
                sql("SELECT COUNT(*), ${colName} FROM dict_table_0 GROUP BY ${colName}")
                contains "VDecode Node"
            }
        }

        for (int i = 0; i < tableCount; ++i) {
            def tableName = """dict_table_${i}"""
            def tmp = i % 10 + 1
            def colName = """col_${tmp}"""
            qt_aggregate """ SELECT COUNT(*), ${colName} FROM ${tableName} GROUP BY ${colName} ORDER BY ${colName} """
        }

        explain {
            sql("SELECT COUNT(*) FROM dict_table_0 GROUP BY SUBSTRING( col_1, 1, 2 )")
            notContains "VDecode Node"
        }

        explain {
            sql("SELECT MAX(col_1) FROM dict_table_0 GROUP BY col_1")
            notContains "VDecode Node"
        }

        explain {
            sql("SELECT MIN(col_1) FROM dict_table_0 GROUP BY col_1")
            notContains "VDecode Node"
        }
    } finally {
        // remove all global dicts
        for (int i = 0; i < tableCount; ++i) {
            def tableName = """dict_table_${i}"""
            for (int j = 1; j <= 10; ++j) {
                def colName = """col_${j}"""
                sql """ ALTER TABLE ${tableName} MODIFY COLUMN ${colName} LOW_CARDINALITY FALSE """
            }
        }

        // verify dict_table_0's global dict info is removed
        for (int j = 1; j <= 10; ++j) {
            def colName = """col_${j}"""
            explain {
                sql("SELECT COUNT(*), ${colName} FROM dict_table_0 GROUP BY ${colName}")
                notContains "VDecode Node"
            }
        }

        for (int i = 0; i < tableCount; ++i) {
            def tableName = """dict_table_${i}"""
            for (int n = 0; n < 20; ++n) {
                def tmp = n % 10
                def strVal = """'value_${tmp}'"""
                sql """ INSERT INTO ${tableName} VALUES( 0, ${strVal}, ${strVal}, ${strVal}, ${strVal}, ${strVal}, 
                                                            ${strVal}, ${strVal}, ${strVal}, ${strVal}, ${strVal} ) """
            }
        }

        for (int i = 0; i < tableCount; ++i) {
            def tableName = """dict_table_${i}"""
            def tmp = i % 10 + 1
            def colName = """col_${tmp}"""
            qt_aggregate """ SELECT COUNT(*), ${colName} FROM ${tableName} GROUP BY ${colName} ORDER BY ${colName} """
        }

        for (int i = 0; i < tableCount; ++i) {
            def tableName = """dict_table_${i}"""
            sql """ DROP TABLE IF EXISTS ${tableName} """
        }
    }
}