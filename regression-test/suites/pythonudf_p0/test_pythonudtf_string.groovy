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

suite("test_pythonudtf_string") {
    def tableName = "test_pythonudtf_string"
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""

    log.info("Python Zip path ${pyPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`     INT         NOT NULL COMMENT "用户id",
            `char_col`    CHAR        NOT NULL COMMENT "",
            `varchar_col` VARCHAR(10) NOT NULL COMMENT "",
            `string_col`  STRING      NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 9; i ++) {
            sb.append("""
                (${i % 3}, '${i}','abc,defg','poiuytre,abcdefg'),
            """)
        }
        sb.append("""
                (${i}, '${i}','ab,cdefg','poiuytreabcde,fg')
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY char_col; """

        File path = new File(pyPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${pyPath} doesn't exist! """)
        }

        sql """DROP FUNCTION IF EXISTS udtf_string_split(string, string);"""

        sql """ CREATE TABLES FUNCTION udtf_string_split(string, string) RETURNS array<string> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="string_test",
            "type"="PYTHON_UDF"
        ); """

        qt_select1 """ SELECT user_id, varchar_col, e1 FROM ${tableName} lateral view  udtf_string_split(varchar_col, ",") temp as e1 order by user_id; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS udtf_string_split(string, string);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
