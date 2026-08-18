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

// What a row policy means must not depend on the sql_mode of whoever wrote it.
//
// Two bits of sql_mode change what SQL text means - PIPES_AS_CONCAT turns || from OR into string
// concatenation, NO_BACKSLASH_ESCAPES changes how a string literal decodes - and sql_mode is a session
// variable any account may set with no privilege at all. A policy's predicate is re-read under one fixed
// mode every time a query it restricts is planned, on the thread of the very user it restricts, so reading
// it under the creator's mode when the statement was accepted meant a policy could be shown as one
// predicate and enforced as another, or accepted and then fail on every query it governed.
suite("test_row_policy_sql_mode") {
    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "row_policy_sql_mode_tbl"
    def user = 'row_policy_sql_mode_user'
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (region varchar(8), path varchar(64))
        DISTRIBUTED BY HASH(region) BUCKETS 1 PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO ${tableName} VALUES ('cn', 'a'), ('us', 'b'), ('de', 'c')"""

    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '123abc!@#'"
    sql "GRANT SELECT_PRIV ON ${dbName}.${tableName} TO ${user}"

    def cloudMode = isCloudMode()
    if (cloudMode) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${user}"""
    }

    def dropPolicy = { name ->
        sql "DROP ROW POLICY IF EXISTS ${name} ON ${dbName}.${tableName} FOR ${user}"
    }

    dropPolicy "p_pipes"
    dropPolicy "p_escapes"

    // || in a policy is OR, whatever the creator's session made it mean. Under PIPES_AS_CONCAT the same
    // text reads as region = ('cn' || region) = 'us' - a comparison against a concatenation, which filters
    // something else entirely - and the query is planned with the OR either way.
    sql "SET sql_mode = 'PIPES_AS_CONCAT'"
    sql """
        CREATE ROW POLICY p_pipes ON ${dbName}.${tableName}
        AS PERMISSIVE TO ${user} USING (region = 'cn' || region = 'us')
    """
    sql "SET sql_mode = DEFAULT"

    // SHOW ROW POLICY has to render what the query is filtered by, not what the creator's session read.
    def shown = sql "SHOW ROW POLICY FOR ${user}"
    def predicate = shown.find { it[0] == 'p_pipes' }[6].toString()
    assertTrue(predicate.toUpperCase().contains(" OR "),
            "SHOW ROW POLICY renders a predicate the query is not filtered by: ${predicate}")

    def filtered = connect(user, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName} ORDER BY region"
    }
    assertEquals(2, filtered.size())

    dropPolicy "p_pipes"

    // Text only the creator's mode can read is refused where it is written. 'C:\' is a complete string
    // literal under NO_BACKSLASH_ESCAPES and an unterminated one without it: accepting it would store a
    // policy that parses on no query at all, and every statement touching the table would fail from then on
    // for every user it applies to.
    sql "SET sql_mode = 'NO_BACKSLASH_ESCAPES'"
    test {
        sql """
            CREATE ROW POLICY p_escapes ON ${dbName}.${tableName}
            AS PERMISSIVE TO ${user} USING (path <> 'C:\\')
        """
        exception "sql_mode"
    }
    sql "SET sql_mode = DEFAULT"

    assertTrue(sql("SHOW ROW POLICY FOR ${user}").every { it[0] != 'p_escapes' },
            "a policy that can be read on no query was stored anyway")

    sql "DROP USER IF EXISTS ${user}"
    sql "DROP TABLE IF EXISTS ${tableName}"
}
