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

suite("test_struct_nested_comment_ddl", "p0") {
    sql "DROP TABLE IF EXISTS struct_nested_comment"
    sql "DROP TABLE IF EXISTS struct_nested_comment_replay"
    sql "DROP TABLE IF EXISTS struct_nested_comment_like"

    // The comment on b carries a single quote and a back slash, both have to survive the DDL.
    sql """
        CREATE TABLE struct_nested_comment (
            id INT,
            s STRUCT<a:INT, b:TEXT COMMENT "owner''s \\\\path", c:INT> COMMENT "top-level",
            n STRUCT<lvl1:STRUCT<lvl2:INT COMMENT "deep doc">>,
            arr ARRAY<STRUCT<inside:INT COMMENT "in array">>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    // Only compare column definitions; table properties depend on the deployment and FE configuration.
    def columnDefinitions = { row ->
        def ddl = row[1].toString()
        [row[0], ddl.substring(ddl.indexOf("(\n") + 2, ddl.indexOf("\n) ENGINE="))]
    }

    // SHOW CREATE TABLE used to drop every nested field comment.
    quickRunTest("show_create", "SHOW CREATE TABLE struct_nested_comment", false, columnDefinitions)
    def createStmt = (sql "SHOW CREATE TABLE struct_nested_comment")[0][1].toString()

    // The printed DDL still has to parse back.
    def replayStmt = createStmt.replace("`struct_nested_comment`", "`struct_nested_comment_replay`")
    sql replayStmt
    quickRunTest("show_create_replay", "SHOW CREATE TABLE struct_nested_comment_replay", false, columnDefinitions)

    // CREATE TABLE LIKE re-parses the same generated DDL, so a dropped nested comment is
    // gone from the new table for good, not just hidden from the user.
    sql "CREATE TABLE struct_nested_comment_like LIKE struct_nested_comment"
    quickRunTest("show_create_like", "SHOW CREATE TABLE struct_nested_comment_like", false, columnDefinitions)
}
