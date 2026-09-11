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

package org.apache.doris.qe.protocol;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.service.arrowflight.FlightSqlConnectProcessor;
import org.apache.doris.service.arrowflight.results.FlightSqlResultCacheEntry;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Records what an Arrow Flight SQL session gets back for the statements a frontend answers itself,
 * and compares it with a golden file.
 *
 * <p>Counterpart of {@link MysqlPacketGoldenTest}: the two protocols are being moved onto one
 * session and one result path, so both ends need a baseline taken before the move. What a Flight
 * client sees is the cached {@code VectorSchemaRoot} -- its schema and its rows -- or the error the
 * statement failed with, so that is what this records.
 *
 * <p>Only statements a frontend can answer are covered. An Arrow Flight session never handles a
 * query in the frontend ({@code ConnectContext.supportHandleByFe()} is false for it), so a
 * {@code SELECT} reaches a backend and produces endpoints instead of a cached result; those belong
 * in the {@code arrow_flight_sql_p0} regression suite.
 *
 * <p>What is covered: {@code SHOW VARIABLES}, {@code SHOW DATABASES} and {@code DESC} (results the
 * frontend materializes and caches), {@code SET} and {@code USE} (no cached result, which is how the
 * producer knows to synthesize its {@code StatusResult=0} row), {@code EXPLAIN}, a syntax error and
 * an unknown table. Every column comes back as {@code Utf8} today -- that is the current behavior,
 * and typing those results is a later step of the same work.
 *
 * <p>Regenerate from the {@code fe} directory with:
 * {@code mvn test -pl fe-common,fe-core -am -Dtest=FlightResultGoldenTest
 * -Ddoris.protocol.golden.regenerate=true} -- {@code -am} is required, the reactor does not
 * resolve {@code ${revision}} without it.
 */
public class FlightResultGoldenTest extends TestWithFeService {
    private static final String GOLDEN_FILE = "flight-results.txt";
    private static final String DB_NAME = "protocol_golden_flight_db";
    private static final String TABLE_NAME = "golden_tbl";
    private static final String PEER_IDENTITY = "protocol-golden-peer";
    private static final int MESSAGE_PREFIX_LENGTH = 72;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse(DB_NAME);
        createTable("create table " + TABLE_NAME + " (k1 int, k2 varchar(32)) duplicate key(k1)"
                + " distributed by hash(k1) buckets 1 properties('replication_num' = '1');");
    }

    @Test
    public void testFlightResultGolden() throws Exception {
        StringBuilder actual = new StringBuilder();
        for (FlightCase flightCase : cases()) {
            actual.append(render(flightCase.statement, flightCase.detail));
        }
        ProtocolGolden.verify(GOLDEN_FILE, actual.toString());
    }

    private List<FlightCase> cases() {
        return Lists.newArrayList(
                new FlightCase("show variables like 'wait_timeout'", Detail.ROWS),
                new FlightCase("show databases like '" + DB_NAME + "'", Detail.ROWS),
                new FlightCase("desc " + TABLE_NAME, Detail.ROWS),
                new FlightCase("set sql_select_limit = 100", Detail.ROWS),
                new FlightCase("use " + DB_NAME, Detail.ROWS),
                // The plan text and the node ids inside it move with the planner, so only the shape
                // of the answer is recorded. Same reason as ProtocolGolden.Fidelity.SUMMARY.
                new FlightCase("explain select 1", Detail.SHAPE),
                // A parser error carries the whole keyword list of the grammar.
                new FlightCase("select from", Detail.SHAPE),
                new FlightCase("select * from no_such_table", Detail.ROWS));
    }

    private String render(String statement, Detail detail) throws Exception {
        ConnectContext ctx = newContext();
        StringBuilder rendered = new StringBuilder();
        rendered.append("=== statement ").append(statement).append(" ===\n");
        try (FlightSqlConnectProcessor processor = new FlightSqlConnectProcessor(ctx)) {
            processor.handleQuery(statement);
            rendered.append(renderOutcome(ctx, detail));
        } finally {
            connectContext.setThreadLocalInfo();
        }
        rendered.append('\n');
        return rendered.toString();
    }

    private String renderOutcome(ConnectContext ctx, Detail detail) {
        StringBuilder rendered = new StringBuilder();
        if (ctx.getState().getStateType() == MysqlStateType.ERR) {
            String message = ctx.getState().getErrorMessage().replace("\n", "\\n").replace("\r", "\\r");
            if (detail == Detail.SHAPE && message.length() > MESSAGE_PREFIX_LENGTH) {
                message = message.substring(0, MESSAGE_PREFIX_LENGTH) + "...";
            }
            rendered.append("state: ERR errorCode=").append(ctx.getState().getErrorCode())
                    .append(" message=\"").append(message).append("\"\n");
            return rendered.toString();
        }
        rendered.append("state: ").append(ctx.getState().getStateType()).append('\n');
        if (ctx.getFlightSqlChannel().resultNum() == 0) {
            // DorisFlightSqlProducer answers such a statement with a synthesized one-row
            // StatusResult=0; nothing was cached by the frontend itself.
            rendered.append("result: none, the producer synthesizes StatusResult=0\n");
            return rendered.toString();
        }
        FlightSqlResultCacheEntry entry = ctx.getFlightSqlChannel().getResult(DebugUtil.printId(ctx.queryId()));
        if (entry == null) {
            rendered.append("result: cached under an unexpected query id\n");
            return rendered.toString();
        }
        rendered.append(renderRoot(entry.getVectorSchemaRoot(), detail));
        return rendered.toString();
    }

    private String renderRoot(VectorSchemaRoot root, Detail detail) {
        StringBuilder rendered = new StringBuilder();
        rendered.append("schema:\n");
        for (Field field : root.getSchema().getFields()) {
            rendered.append("  ").append(field.getName()).append(": ").append(field.getType())
                    .append(field.isNullable() ? " nullable" : " not null").append('\n');
        }
        if (detail == Detail.SHAPE) {
            rendered.append("rows: ").append(root.getRowCount() > 0 ? "some" : "none").append('\n');
            return rendered.toString();
        }
        rendered.append("rows: ").append(root.getRowCount()).append('\n');
        for (int row = 0; row < root.getRowCount(); row++) {
            StringBuilder line = new StringBuilder();
            for (FieldVector vector : root.getFieldVectors()) {
                if (line.length() > 0) {
                    line.append(" | ");
                }
                line.append(valueOf(vector, row));
            }
            // Trailing spaces would be an empty last column; keep them out of the file so an editor
            // or a whitespace lint cannot silently rewrite the golden.
            rendered.append("  ").append(line.toString().stripTrailing()).append('\n');
        }
        return rendered.toString();
    }

    private String valueOf(FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return "NULL";
        }
        if (vector instanceof VarCharVector) {
            return new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8);
        }
        return String.valueOf(vector.getObject(row));
    }

    /** How much of a result to keep; see ProtocolGolden.Fidelity for why the second mode exists. */
    private enum Detail {
        ROWS,
        SHAPE
    }

    private static class FlightCase {
        private final String statement;
        private final Detail detail;

        FlightCase(String statement, Detail detail) {
            this.statement = statement;
            this.detail = detail;
        }
    }

    private ConnectContext newContext() {
        ConnectContext ctx = ConnectContext.forFlight(PEER_IDENTITY);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setRemoteIP("127.0.0.1");
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setDatabase(DB_NAME);
        ctx.setThreadLocalInfo();
        return ctx;
    }
}
