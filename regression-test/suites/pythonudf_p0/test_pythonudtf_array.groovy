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

suite("test_pythonudtf_array") {
    def tableName = "test_pythonudtf_array"
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""
    scp_udf_file_to_all_be(pyPath)

    log.info("Python Zip path: ${pyPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`      INT      NOT NULL COMMENT "",
            `int_col`  INT  NOT NULL COMMENT "",
            `string_col`   STRING   NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 3; i++) {
            sb.append("""
                (${i},${i}*2,'a${i}b'),
            """)
        }
        sb.append("""
                (${i},${i}*2,'a${i}b')
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(pyPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${pyPath} doesn't exist! """)
        }

        sql """ DROP FUNCTION IF EXISTS udtf_array_int_test(INT); """
        
        sql """ CREATE TABLES FUNCTION udtf_array_int_test(INT) RETURNS ARRAY<ARRAY<INT>> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="array_int_test",
            "type"="PYTHON_UDF"
        ); """
        qt_select_1 """ SELECT user_id, e1 FROM ${tableName} LATERAL VIEW udtf_array_int_test(int_col) temp as e1 ORDER BY user_id; """


        sql """ DROP FUNCTION IF EXISTS udtf_array_string_test(STRING); """
        sql """ CREATE TABLES FUNCTION udtf_array_string_test(STRING) RETURNS ARRAY<ARRAY<STRING>> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="array_string_test",
            "type"="PYTHON_UDF"
        ); """
        qt_select_2 """ SELECT user_id, e1 FROM ${tableName} LATERAL VIEW udtf_array_string_test(string_col) temp as e1 ORDER BY user_id; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS udtf_array_int_test(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_array_string_test(STRING);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
