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

suite("test_pythonudtf_int") {
    def tableName = "test_pythonudtf_int"
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""

    log.info("Python Zip path: ${pyPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `id1`     TINYINT         NOT NULL COMMENT "",
            `id2`    SMALLINT        NOT NULL COMMENT "",
            `id3`    INT        NOT NULL COMMENT "",
            `id4`    BIGINT        NOT NULL COMMENT "",
            )
            DISTRIBUTED BY HASH(id1) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 9; i ++) {
            sb.append("""
                (${i % 3}, '${i}','${i * 100}','${i * 1000}'),
            """)
        }
        sb.append("""
                (${i % 3}, '${i}','${i * 100}','${i * 1000}')
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY id1; """

        File path = new File(pyPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${pyPath} doesn't exist! """)
        }

        sql """DROP FUNCTION IF EXISTS udtf_tinyint(TINYINT);"""
        sql """DROP FUNCTION IF EXISTS udtf_smallint(SMALLINT);"""
        sql """DROP FUNCTION IF EXISTS udtf_int(INT);"""
        sql """DROP FUNCTION IF EXISTS udtf_bigint(BIGINT);"""

        sql """ CREATE TABLES FUNCTION udtf_tinyint(TINYINT) RETURNS array<TINYINT> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="int_test",
            "type"="PYTHON_UDF"
        ); """
        sql """ CREATE TABLES FUNCTION udtf_smallint(SMALLINT) RETURNS array<SMALLINT> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="int_test",
            "type"="PYTHON_UDF"
        ); """
        sql """ CREATE TABLES FUNCTION udtf_int(INT) RETURNS array<INT> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="int_test",
            "type"="PYTHON_UDF"
        ); """
        sql """ CREATE TABLES FUNCTION udtf_bigint(BIGINT) RETURNS array<BIGINT> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="int_test",
            "type"="PYTHON_UDF"
        ); """

        qt_select1 """ SELECT id1, e1 FROM ${tableName} lateral view udtf_tinyint(id1) temp as e1 order by id1; """
        qt_select2 """ SELECT id2, e2 FROM ${tableName} lateral view udtf_smallint(id2) temp as e2 order by id2; """
        qt_select3 """ SELECT id3, e3 FROM ${tableName} lateral view udtf_int(id3) temp as e3 order by id3; """
        qt_select4 """ SELECT id4, e4 FROM ${tableName} lateral view udtf_bigint(id4) temp as e4 order by id4; """
    } finally {
        try_sql("DROP FUNCTION IF EXISTS udtf_tinyint(TINYINT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_smallint(SMALLINT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_int(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_bigint(BIGINT);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
