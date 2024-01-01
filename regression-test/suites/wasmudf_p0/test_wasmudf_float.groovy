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

suite("test_wasmudf_float") {
    def tableName = "test_wasmudf_float"
    def watPath = """${context.file.parent}/wat/f32_add.wat"""

    log.info("Wat path: ${watPath}".toString())
    try {
        try_sql("DROP FUNCTION IF EXISTS wasm_udf_float_test(FLOAT,FLOAT);")

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`  INT    NOT NULL COMMENT "",
            `float_1`  FLOAT  NOT NULL COMMENT "",
            `float_2`  FLOAT           COMMENT "",
            `double_1` DOUBLE NOT NULL COMMENT "",
            `double_2` DOUBLE          COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        
        
        sql """ INSERT INTO ${tableName} (`user_id`,`float_1`,`float_2`,double_1,double_2) VALUES
                (111,11111.11111,222222.3333333,12345678.34455677,1111111.999999999999),
                (112,1234556.11111,222222.3333333,222222222.3333333333333,4444444444444.555555555555),
                (113,87654321.11111,null,6666666666.6666666666,null)
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(watPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${watPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION wasm_udf_float_test(FLOAT,FLOAT) RETURNS FLOAT PROPERTIES (
            "file"="file://${watPath}",
            "symbol"="add",
            "type"="WASM_UDF",
            "always_nullable"="true"
        ); """

        qt_select """ SELECT wasm_udf_float_test(cast(2.83645 as float),cast(111.1111111 as float)) as result; """
        qt_select """ SELECT wasm_udf_float_test(2.83645,111.1111111) as result ; """
        qt_select """ SELECT wasm_udf_float_test(2.83645,null) as result ; """
        qt_select """ SELECT wasm_udf_float_test(cast(2.83645 as float),null) as result ; """
        qt_select """ SELECT user_id,wasm_udf_float_test(float_1, float_2) as sum FROM ${tableName} order by user_id; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS wasm_udf_float_test(FLOAT,FLOAT);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
