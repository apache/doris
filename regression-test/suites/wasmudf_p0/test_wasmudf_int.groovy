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

suite("test_wasmudf_int") {
    def tableName = "test_wasmudf_int"
    def watPath = """${context.file.parent}/wat/i32_add.wat"""

    log.info("Wat path: ${watPath}".toString())
    try {
        try_sql("DROP FUNCTION IF EXISTS wasm_udf_int_add_test(int, int);")

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`      INT      NOT NULL COMMENT "",
            `tinyint_col`  TINYINT  NOT NULL COMMENT "",
            `smallint_col` SMALLINT NOT NULL COMMENT "",
            `bigint_col`   BIGINT   NOT NULL COMMENT "",
            `largeint_col` LARGEINT NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i ++) {
            sb.append("""
                (${i},${i}*2,${i}*3,${i}*4,${i}*5),
            """)
        }
        sb.append("""
                (${i},${i}*2,${i}*3,${i}*4,${i}*5)
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(watPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${watPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION wasm_udf_int_add_test(int, int) RETURNS int PROPERTIES (
            "file"="file://${watPath}",
            "symbol"="add",
            "type"="WASM_UDF",
            "always_nullable"="true"
        ); """

        qt_select """ SELECT wasm_udf_int_add_test(user_id, user_id) result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT wasm_udf_int_add_test(user_id, null) result FROM ${tableName} ORDER BY result; """



    } finally {
        try_sql("DROP FUNCTION IF EXISTS wasm_udf_int_add_test(int, int);")
        try_sql("DROP TABLE IF EXISTS ${tableName};")
    }
}
