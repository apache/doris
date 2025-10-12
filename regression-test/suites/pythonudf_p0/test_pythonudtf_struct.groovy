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

suite("test_pythonudtf_struct") {
    def tableName = "test_pythonudtf_struct"
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""

    log.info("Python Zip path: ${pyPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `id`     INT         NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 3; i++) {
            sb.append("""
                (${i % 3}),
            """)
        }
        sb.append("""(${i % 3})""")
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY id; """

        sql """DROP FUNCTION IF EXISTS udtf_struct(INT);"""
        sql """ CREATE TABLES FUNCTION udtf_struct(INT) RETURNS ARRAY<STRUCT<field1:INT, field2:FLOAT, field3:STRING>>
             PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="struct_test",
            "type"="PYTHON_UDF"
        ); """
        qt_select1 """ SELECT id, field1, field2, field3 FROM ${tableName} lateral view udtf_struct(id) temp as 
                       field1, field2, field3 order by id; """
    } finally {
        try_sql("DROP FUNCTION IF EXISTS udtf_struct(INT);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
