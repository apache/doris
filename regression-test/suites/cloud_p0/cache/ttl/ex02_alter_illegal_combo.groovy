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

suite("ex02_alter_illegal_combo") {
    if (!isCloudMode()) {
        return
    }

    def clusters = sql "SHOW CLUSTERS"
    assertTrue(!clusters.isEmpty())
    def validCluster = clusters[0][0]
    sql """use @${validCluster};"""

    String tableName = "ex02_alter_illegal_combo"
    def ddl = new File("""${context.file.parent}/../ddl/ex02_alter_illegal_combo.sql""").text
            .replace("\${TABLE_NAME}", tableName)
    sql ddl

    try {
        sql """insert into ${tableName} values (1, 'a'), (2, 'b'), (3, 'c')"""
        qt_ex02_base """select count(*) from ${tableName}"""

        // EX-02: 一次 ALTER 同时改多个属性应报错。
        try {
            sql """
                alter table ${tableName}
                set (
                    "file_cache_ttl_seconds" = "120",
                    "disable_auto_compaction" = "false"
                )
            """
            assertTrue(false, "alter with multiple properties should fail")
        } catch (Exception e) {
            def msg = e.getMessage()?.toLowerCase()
            assertTrue(msg != null && msg.contains("one table property"),
                    "unexpected error: ${e.getMessage()}")
        }

        def showRows = sql """show create table ${tableName}"""
        assertTrue(!showRows.isEmpty())
        String createSql = showRows[0][1].toString()
        assertTrue(createSql.contains("\"file_cache_ttl_seconds\" = \"300\""),
                "ttl should keep original value after failed alter")
    } finally {
        sql """drop table if exists ${tableName}"""
    }
}

