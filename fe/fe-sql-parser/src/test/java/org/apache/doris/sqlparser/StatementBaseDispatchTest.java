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

package org.apache.doris.sqlparser;

import org.apache.doris.nereids.DorisParser.MultiStatementsContext;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.exceptions.ParseException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

class StatementBaseDispatchTest {
    private final DorisSqlParser parser = new DorisSqlParser();

    @ParameterizedTest(name = "{0}")
    @MethodSource("statementFamilies")
    void parsesEveryStatementFamily(String family, String sql) {
        assertParses(sql);
    }

    private static Stream<Arguments> statementFamilies() {
        return Stream.of(
                Arguments.of("query", "SELECT 1"),
                Arguments.of("dml", "INSERT INTO t SELECT 1"),
                Arguments.of("create", "CREATE DATABASE db"),
                Arguments.of("alter", "ALTER TABLE t RENAME t2"),
                Arguments.of("materialized view", "CREATE MATERIALIZED VIEW mv AS SELECT 1"),
                Arguments.of("job", "PAUSE JOB WHERE jobName = 'job'"),
                Arguments.of("constraint", "SHOW CONSTRAINTS FROM t"),
                Arguments.of("clean", "CLEAN ALL PROFILE"),
                Arguments.of("describe", "DESCRIBE t"),
                Arguments.of("drop", "DROP TABLE t"),
                Arguments.of("set", "SET x = 1"),
                Arguments.of("unset", "UNSET VARIABLE x"),
                Arguments.of("refresh", "REFRESH TABLE t"),
                Arguments.of("show", "SHOW TABLES"),
                Arguments.of("load", "SYNC"),
                Arguments.of("cancel", "CANCEL LOAD"),
                Arguments.of("recover", "RECOVER TABLE t"),
                Arguments.of("admin", "ADMIN CLEAN TRASH"),
                Arguments.of("use", "USE db"),
                Arguments.of("other", "HELP 'SHOW'"),
                Arguments.of("kill", "KILL 1"),
                Arguments.of("stats", "ANALYZE TABLE t"),
                Arguments.of("transaction", "BEGIN"),
                Arguments.of("grant/revoke", "GRANT ALL ON db.t TO 'user'@'%'"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("sharedPrefixStatements")
    void parsesSharedFirstTokenStatements(String branch, String sql) {
        assertParses(sql);
    }

    private static Stream<Arguments> sharedPrefixStatements() {
        return Stream.of(
                Arguments.of("CREATE ddl", "CREATE DATABASE db"),
                Arguments.of("CREATE mv", "CREATE MATERIALIZED VIEW mv AS SELECT 1"),
                Arguments.of("CREATE job",
                        "CREATE JOB db.job ON SCHEDULE AT '2026-01-01 00:00:00' DO INSERT INTO t SELECT 1"),
                Arguments.of("CREATE load",
                        "CREATE ROUTINE LOAD db.job ON t PROPERTIES (\"max_batch_interval\" = \"10\") "
                                + "FROM KAFKA (\"kafka_broker_list\" = \"localhost:9092\")"),
                Arguments.of("ALTER ddl", "ALTER TABLE t RENAME t2"),
                Arguments.of("ALTER mv", "ALTER MATERIALIZED VIEW mv RENAME mv2"),
                Arguments.of("ALTER job", "ALTER JOB db.job PROPERTIES (\"k\" = \"v\")"),
                Arguments.of("ALTER constraint", "ALTER TABLE t ADD CONSTRAINT pk PRIMARY KEY (id)"),
                Arguments.of("ALTER stats", "ALTER TABLE t SET STATS (\"row_count\" = \"1\")"),
                Arguments.of("SHOW command", "SHOW TABLES"),
                Arguments.of("SHOW mv", "SHOW CREATE MATERIALIZED VIEW mv"),
                Arguments.of("SHOW constraint", "SHOW CONSTRAINTS FROM t"),
                Arguments.of("SHOW load", "SHOW ROUTINE LOAD"),
                Arguments.of("SHOW stats", "SHOW ANALYZE"),
                Arguments.of("DROP ddl", "DROP TABLE t"),
                Arguments.of("DROP mv", "DROP MATERIALIZED VIEW mv"),
                Arguments.of("DROP job", "DROP JOB WHERE jobName = 'job'"),
                Arguments.of("DROP stats", "DROP STATS t"),
                Arguments.of("PAUSE mv", "PAUSE MATERIALIZED VIEW JOB ON mv"),
                Arguments.of("PAUSE job", "PAUSE JOB WHERE jobName = 'job'"),
                Arguments.of("PAUSE load", "PAUSE ROUTINE LOAD FOR db.job"),
                Arguments.of("RESUME mv", "RESUME MATERIALIZED VIEW JOB ON mv"),
                Arguments.of("RESUME job", "RESUME JOB WHERE jobName = 'job'"),
                Arguments.of("RESUME load", "RESUME ROUTINE LOAD FOR db.job"),
                Arguments.of("CANCEL mv", "CANCEL MATERIALIZED VIEW TASK 1 ON mv"),
                Arguments.of("CANCEL job", "CANCEL TASK WHERE jobName = 'job' AND taskId = 1"),
                Arguments.of("CANCEL command", "CANCEL LOAD"),
                Arguments.of("REFRESH mv", "REFRESH MATERIALIZED VIEW mv COMPLETE"),
                Arguments.of("REFRESH command", "REFRESH TABLE t"),
                Arguments.of("KILL connection", "KILL 1"),
                Arguments.of("KILL analyze", "KILL ANALYZE 1"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("delayedDispatchStatements")
    void parsesStatementsWhoseBranchNeedsMoreLookahead(String branch, String sql) {
        assertParses(sql);
    }

    private static Stream<Arguments> delayedDispatchStatements() {
        return Stream.of(
                Arguments.of("EXPLAIN query", "EXPLAIN SELECT 1"),
                Arguments.of("EXPLAIN dml", "EXPLAIN INSERT INTO t SELECT 1"),
                Arguments.of("WITH query", "WITH c AS (SELECT 1) SELECT * FROM c"),
                Arguments.of("WITH dml", "WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c"),
                Arguments.of("parenthesized query", "(SELECT 1)"),
                Arguments.of("query outfile", "SELECT 1 INTO OUTFILE 'file:///tmp/result'"),
                Arguments.of("warm-up explain", "EXPLAIN WARM UP SELECT * FROM t"),
                Arguments.of("describe", "DESC t"));
    }

    @ParameterizedTest(name = "truncated: {0}")
    @MethodSource("truncatedStatements")
    void rejectsTruncatedSharedPrefixesAtEndOfInput(String sql) {
        ParseException exception = Assertions.assertThrows(ParseException.class, () -> parser.parseStatement(sql));
        Assertions.assertTrue(exception.getMessage().contains("line 1, pos " + sql.length()), exception::getMessage);
    }

    private static Stream<String> truncatedStatements() {
        return Stream.of("CREATE", "ALTER TABLE", "SHOW", "DROP", "ADMIN", "CANCEL", "REFRESH", "KILL");
    }

    @Test
    void parsesMultiStatementsWithCommentsAndExtraSemicolons() {
        String sql = "; /* leading */ SELECT 1;; CREATE DATABASE db; -- comment\n SHOW TABLES;;";
        MultiStatementsContext context = parser.parseStatements(sql);
        Assertions.assertEquals(3, context.statement().size());
    }

    @Test
    void rejectsInvalidMiddleStatement() {
        String sql = "SELECT 1; CREATE; SHOW TABLES";
        ParseException exception = Assertions.assertThrows(ParseException.class, () -> parser.parseStatements(sql));
        Assertions.assertTrue(exception.getMessage().contains("line 1, pos 16"), exception::getMessage);
    }

    private void assertParses(String sql) {
        SingleStatementContext context = parser.parseStatement(sql);
        Assertions.assertNotNull(context.statement());
    }
}
