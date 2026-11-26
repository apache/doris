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

// WHY: LIKE pushdown to paimon may only ever WIDEN what the source returns, never narrow it. The pushed
// predicate drives paimon's partition and data-file pruning at planning time, so a prefix stricter than the
// user's pattern makes paimon skip files holding matching rows - and the BE-side residual LIKE filter can only
// drop rows from what was read, never read a skipped file back. The result is a query that silently returns
// fewer rows, with no error and no EXPLAIN signal.
//
// The connector used to treat any pattern that did not start with '%' and did end with '%' as a literal prefix
// match, ignoring three things Doris LIKE means:
//   * '_' is a single-character wildcard, so 'a_c%' must also match 'abc1';
//   * '\' escapes the next character, so 'a\%%' means "starts with the literal a%", not "starts with a\%";
//   * a '%' left in the middle is still a wildcard, so 'a%b%' is not the literal prefix 'a%b'.
//
// Each value is written in its own INSERT so the rows land in separate data files and file-level pruning is
// deterministically reachable; otherwise a single file would be read regardless and the bug would hide.
suite("test_paimon_like_pushdown", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_paimon_like_pushdown"
    String dbName = "test_paimon_like_pushdown_db"
    String tableName = "like_pushdown_tbl"

    spark_paimon_multi """
        create database if not exists paimon.${dbName};
        drop table if exists paimon.${dbName}.${tableName};
        create table paimon.${dbName}.${tableName} (s string) using paimon;
        insert into paimon.${dbName}.${tableName} values ('abc1');
        insert into paimon.${dbName}.${tableName} values ('a_c1');
        insert into paimon.${dbName}.${tableName} values ('a%b1');
    """

    sql """drop catalog if exists ${catalog_name}"""
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
                'type' = 'paimon',
                'warehouse' = 's3://warehouse/wh',
                's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.path.style.access' = 'true'
        );
    """
    sql """use `${catalog_name}`.`${dbName}`"""

    // Assertions are inline rather than qt_/.out baselines: the expected answer here is the plain meaning
    // of LIKE, which is worth stating in the test rather than recording from a run.
    def matched = { String pattern ->
        sql("select s from ${tableName} where s like '${pattern}' order by s").collect { it[0] }
    }

    // '_' is a wildcard: both 'a_c1' and 'abc1' match. Before the fix the connector pushed
    // startsWith("a_c") and paimon pruned away the file holding 'abc1'.
    assertEquals(["a_c1", "abc1"], matched("a_c%"))

    // '\%' is an escaped literal '%': only 'a%b1' matches. Before the fix the connector pushed
    // startsWith("a\\%") - backslash included - which typically matches nothing at all.
    assertEquals(["a%b1"], matched("a\\\\%b%"))

    // A '%' in the middle stays a wildcard: 'a%b1' and 'abc1' both match ('abc1' has a 'b' after the 'a').
    // Before the fix the connector pushed startsWith("a%b") as a literal.
    assertEquals(["a%b1", "abc1"], matched("a%b%"))

    // The one shape that IS provably a prefix match. Kept as a guard that tightening the check did not
    // stop pushing the common, useful case.
    assertEquals(["abc1"], matched("abc%"))

    sql """drop catalog if exists ${catalog_name}"""
}
