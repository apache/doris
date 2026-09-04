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

suite("test_call_procedure_not_supported", "arrow_flight_sql") {
    def tableName = "test_call_procedure_not_supported_tbl"
    def procName = "test_call_procedure_not_supported_proc"

    jdbc_sql "DROP TABLE IF EXISTS ${tableName}"
    jdbc_sql """
        CREATE TABLE ${tableName} (id INT, name VARCHAR(20)) DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ("replication_num"="1");
        """
    jdbc_sql """
        CREATE OR REPLACE PROCEDURE ${procName}(IN id INT, IN name STRING)
        BEGIN
            INSERT INTO ${tableName} VALUES(id, name);
        END;
        """

    // The PL/SQL interpreter writes through the session's MySQL channel, which an Arrow Flight SQL
    // connection does not have. Before the guard, the CALL ran its body first and only then hit
    // `getMysqlChannel not in mysql connection`, so the client saw an error while the INSERT had
    // already been applied. The statement must now be refused before anything is executed.
    def rejected = false
    try {
        arrow_flight_sql """CALL ${procName}(444, "adbc444")"""
    } catch (Exception e) {
        rejected = true
        assertTrue(e.getMessage().contains("only supported on the MySQL protocol"),
                "unexpected error message: " + e.getMessage())
    }
    assertTrue(rejected, "CALL of a stored procedure must be rejected on an Arrow Flight SQL connection")

    // The refused CALL must not have applied its side effect.
    def rows = jdbc_sql """SELECT COUNT(*) FROM ${tableName}"""
    assertEquals(0L, rows[0][0] as long)

    // CALL is still served on the MySQL protocol.
    jdbc_sql """CALL ${procName}(555, "jdbc555")"""
    rows = jdbc_sql """SELECT COUNT(*) FROM ${tableName}"""
    assertEquals(1L, rows[0][0] as long)

    jdbc_sql "DROP PROCEDURE IF EXISTS ${procName}"
    jdbc_sql "DROP TABLE IF EXISTS ${tableName}"
}
