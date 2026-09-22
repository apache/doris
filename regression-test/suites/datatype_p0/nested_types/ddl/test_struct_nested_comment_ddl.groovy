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
    sql "DROP TABLE IF EXISTS struct_field_name_quoting"

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

    // SHOW CREATE TABLE used to drop every nested field comment.
    qt_show_create "SHOW CREATE TABLE struct_nested_comment"
    def createStmt = (sql "SHOW CREATE TABLE struct_nested_comment")[0][1].toString()

    // The printed DDL still has to parse back.
    def replayStmt = createStmt.replace("`struct_nested_comment`", "`struct_nested_comment_replay`")
    sql replayStmt
    assertEquals(replayStmt, (sql "SHOW CREATE TABLE struct_nested_comment_replay")[0][1].toString())

    // CREATE TABLE LIKE re-parses the same generated DDL, so a dropped nested comment is
    // gone from the new table for good, not just hidden from the user.
    sql "CREATE TABLE struct_nested_comment_like LIKE struct_nested_comment"
    def likeStmt = (sql "SHOW CREATE TABLE struct_nested_comment_like")[0][1].toString()
    assertEquals(createStmt.replace("`struct_nested_comment`", "`struct_nested_comment_like`"), likeStmt)

    // The comment is escaped for the mode the statement will be read under, so a back slash
    // survives CREATE TABLE LIKE under NO_BACKSLASH_ESCAPES too.
    sql "SET sql_mode = 'NO_BACKSLASH_ESCAPES'"
    def nbseStmt = (sql "SHOW CREATE TABLE struct_nested_comment")[0][1].toString()
    sql "DROP TABLE IF EXISTS struct_nested_comment_like"
    sql "CREATE TABLE struct_nested_comment_like LIKE struct_nested_comment"
    assertEquals(nbseStmt.replace("`struct_nested_comment`", "`struct_nested_comment_like`"),
            (sql "SHOW CREATE TABLE struct_nested_comment_like")[0][1].toString())
    sql "SET sql_mode = ''"

    // Field names holding a separator are legal and have to stay tellable apart in COLUMN_TYPE.
    sql """
        CREATE TABLE struct_field_name_quoting (
            id INT,
            s STRUCT<`a,b`:INT, `c:d`:INT, `e<f>`:INT, plain:INT>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    // COLUMN_TYPE has to describe a struct with its field names.
    sql "use information_schema"
    qt_column_type """
        SELECT column_name, data_type, column_type FROM columns
        WHERE table_name = 'struct_nested_comment' ORDER BY column_name
    """
    qt_column_type_quoting """
        SELECT column_name, column_type FROM columns
        WHERE table_name = 'struct_field_name_quoting' ORDER BY column_name
    """
}
