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
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MysqlConnectProcessor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Records the MySQL packets a set of commands produces and compares them with a golden file.
 *
 * <p>This is the safety net for the protocol-independent session work: the MySQL front end is being
 * pulled apart into a protocol adapter and a result sender, and nothing else in the tree asserts the
 * bytes that leave the server. A refactor that changes a sequence id, drops an EOF, or reorders a
 * column definition passes every other unit test and only shows up as a hung or confused client.
 *
 * <p>The statement set is deliberately limited to what a frontend can answer on its own. Anything
 * that needs a backend belongs in the regression suites, not here -- in particular
 * {@code COM_STMT_EXECUTE}, which {@link org.apache.doris.qe.ConnectContext#supportHandleByFe()}
 * always sends to one, so the cursor-fetch packet boundaries live in {@code prepared_stmt_p0}.
 *
 * <p>What is covered:
 * <ul>
 *   <li>result sets a frontend computes: a literal, two columns, a session variable, a NULL, and an
 *       empty result set;</li>
 *   <li>the same result set with {@code CLIENT_DEPRECATE_EOF} negotiated off, which decides whether
 *       a result set ends in an EOF or an OK, and adds one after the column definitions;</li>
 *   <li>{@code SHOW VARIABLES} (both EOF flavors), {@code DESC} (two data rows), {@code SET},
 *       {@code USE}, {@code EXPLAIN}, {@code EXPLAIN PLAN PROCESS};</li>
 *   <li>errors: a syntax error, an unknown table, and an error followed by a healthy statement on
 *       the same connection;</li>
 *   <li>multi-statement requests with and without {@code CLIENT_MULTI_STATEMENTS}, which decides
 *       whether the intermediate result set gets a terminator at all;</li>
 *   <li>connection commands: {@code COM_FIELD_LIST}, {@code COM_STMT_PREPARE},
 *       {@code COM_STMT_CLOSE}, {@code COM_SET_OPTION}, {@code COM_RESET_CONNECTION},
 *       {@code COM_PING}, {@code COM_INIT_DB}, {@code COM_STATISTICS}, an unknown command, and
 *       {@code COM_QUIT}.</li>
 * </ul>
 *
 * <p>Regenerate from the {@code fe} directory with:
 * {@code mvn test -pl fe-common,fe-core -am -Dtest=MysqlPacketGoldenTest
 * -Ddoris.protocol.golden.regenerate=true} -- {@code -am} is required, the reactor does not
 * resolve {@code ${revision}} without it.
 */
public class MysqlPacketGoldenTest extends TestWithFeService {
    private static final String GOLDEN_FILE = "mysql-packets.txt";
    private static final String DB_NAME = "protocol_golden_db";
    private static final String TABLE_NAME = "golden_tbl";

    private static final int MODERN_CLIENT = MysqlCapability.DEFAULT_CAPABILITY.getFlags();
    private static final int LEGACY_EOF_CLIENT =
            MODERN_CLIENT & ~MysqlCapability.Flag.CLIENT_DEPRECATE_EOF.getFlagBit();
    private static final int MULTI_STATEMENT_CLIENT =
            MODERN_CLIENT | MysqlCapability.Flag.CLIENT_MULTI_STATEMENTS.getFlagBit();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse(DB_NAME);
        createTable("create table " + TABLE_NAME + " (k1 int, k2 varchar(32)) duplicate key(k1)"
                + " distributed by hash(k1) buckets 1 properties('replication_num' = '1');");
    }

    @Test
    public void testMysqlPacketGolden() throws Exception {
        StringBuilder actual = new StringBuilder();
        for (GoldenCase goldenCase : cases()) {
            actual.append(render(goldenCase));
        }
        ProtocolGolden.verify(GOLDEN_FILE, actual.toString());
    }

    private List<GoldenCase> cases() {
        List<GoldenCase> cases = Lists.newArrayList();
        cases.add(new GoldenCase("select-literal", MODERN_CLIENT)
                .add(query("select 1")));
        cases.add(new GoldenCase("select-two-columns", MODERN_CLIENT)
                .add(query("select 1, 'a'")));
        cases.add(new GoldenCase("select-session-variable", MODERN_CLIENT)
                .add(query("select @@wait_timeout")));
        cases.add(new GoldenCase("select-null-literal", MODERN_CLIENT)
                .add(query("select null")));
        cases.add(new GoldenCase("select-empty-result", MODERN_CLIENT)
                .add(query("select k1 from " + TABLE_NAME + " where 1 = 0")));
        cases.add(new GoldenCase("select-literal-legacy-eof", LEGACY_EOF_CLIENT)
                .add(query("select 1")));
        cases.add(new GoldenCase("show-variables", MODERN_CLIENT)
                .add(query("show variables like 'wait_timeout'")));
        cases.add(new GoldenCase("show-variables-legacy-eof", LEGACY_EOF_CLIENT)
                .add(query("show variables like 'wait_timeout'")));
        // Two data rows: the only case here where the row stream is longer than one packet.
        cases.add(new GoldenCase("describe-table", MODERN_CLIENT)
                .add(query("desc " + TABLE_NAME)));
        cases.add(new GoldenCase("set-session-variable", MODERN_CLIENT)
                .add(query("set sql_select_limit = 100")));
        cases.add(new GoldenCase("use-database", MODERN_CLIENT)
                .add(query("use " + DB_NAME)));
        cases.add(new GoldenCase("explain-select", MODERN_CLIENT, ProtocolGolden.Fidelity.SUMMARY)
                .add(query("explain select 1")));
        // The rule names and plan shapes move with the planner, like the plan text above.
        cases.add(new GoldenCase("explain-plan-process", MODERN_CLIENT, ProtocolGolden.Fidelity.SUMMARY)
                .add(query("explain plan process select 1")));
        cases.add(new GoldenCase("syntax-error", MODERN_CLIENT, ProtocolGolden.Fidelity.SUMMARY)
                .add(query("select from")));
        cases.add(new GoldenCase("unknown-table", MODERN_CLIENT)
                .add(query("select * from no_such_table")));
        cases.add(new GoldenCase("error-then-next-command", MODERN_CLIENT)
                .add(query("select * from no_such_table"))
                .add(query("select 1")));
        cases.add(new GoldenCase("multi-statement-with-capability", MULTI_STATEMENT_CLIENT)
                .add(query("select 1; select 2")));
        cases.add(new GoldenCase("multi-statement-without-capability", MODERN_CLIENT)
                .add(query("select 1; select 2")));
        cases.add(new GoldenCase("com-field-list", MODERN_CLIENT)
                .add(fieldList(TABLE_NAME)));
        cases.add(new GoldenCase("com-stmt-prepare", MODERN_CLIENT)
                .add(command("COM_STMT_PREPARE select ?", 0x16, "select ?")));
        cases.add(new GoldenCase("com-stmt-close", MODERN_CLIENT)
                .add(stmtClose(1)));
        cases.add(new GoldenCase("com-set-option", MODERN_CLIENT)
                .add(setOption(0)));
        cases.add(new GoldenCase("com-reset-connection", MODERN_CLIENT)
                .add(command("COM_RESET_CONNECTION", 0x1F, "")));
        cases.add(new GoldenCase("com-ping", MODERN_CLIENT)
                .add(command("COM_PING", 0x0E, "")));
        cases.add(new GoldenCase("com-init-db", MODERN_CLIENT)
                .add(command("COM_INIT_DB " + DB_NAME, 0x02, DB_NAME)));
        cases.add(new GoldenCase("com-statistics", MODERN_CLIENT)
                .add(command("COM_STATISTICS", 0x09, "")));
        cases.add(new GoldenCase("com-unknown", MODERN_CLIENT)
                .add(command("unknown command 0x2A", 0x2A, "")));
        cases.add(new GoldenCase("com-quit", MODERN_CLIENT)
                .add(command("COM_QUIT", 0x01, "")));
        return cases;
    }

    private String render(GoldenCase goldenCase) throws Exception {
        RecordingMysqlChannel channel = new RecordingMysqlChannel();
        ConnectContext ctx = newContext(channel, goldenCase.clientFlags);
        MysqlConnectProcessor processor = new MysqlConnectProcessor(ctx);
        StringBuilder rendered = new StringBuilder();
        rendered.append("=== case ").append(goldenCase.name).append(" ===\n");
        rendered.append("client capability: ").append(describe(goldenCase.clientFlags)).append('\n');
        try {
            for (Command command : goldenCase.commands) {
                channel.clearOutbound();
                channel.queueRequest(command.payload);
                processor.processOnce();
                rendered.append("--> ").append(command.label).append('\n');
                rendered.append(ProtocolGolden.renderPackets(channel.getOutbound(), goldenCase.fidelity));
            }
        } finally {
            // Hand the thread back to the context TestWithFeService installed, so the helpers it
            // offers (create table, drop database) keep working after a case has run.
            connectContext.setThreadLocalInfo();
        }
        rendered.append('\n');
        return rendered.toString();
    }

    private ConnectContext newContext(RecordingMysqlChannel channel, int clientFlags) {
        ConnectContext ctx = new ConnectContext(new MysqlProtocolAdapter(channel));
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setRemoteIP("127.0.0.1");
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setDatabase(DB_NAME);
        ctx.setStatementContext(new StatementContext());
        // Replay what MysqlProto.negotiate() derives from the client's capability flags: the
        // effective capability is the intersection with the server's, and the two channel flags are
        // set from it. Everything the golden records downstream depends on this.
        MysqlCapability clientCapability = new MysqlCapability(clientFlags);
        ctx.setCapability(new MysqlCapability(ctx.getServerCapability().getFlags() & clientCapability.getFlags()));
        if (ctx.getCapability().isDeprecatedEOF()) {
            channel.setClientDeprecatedEOF();
        }
        if (clientCapability.isClientMultiStatements()) {
            channel.setClientMultiStatements();
        }
        channel.getSerializer().setCapability(ctx.getCapability());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    private static String describe(int clientFlags) {
        MysqlCapability capability = new MysqlCapability(clientFlags);
        return "deprecate_eof=" + capability.isDeprecatedEOF()
                + " multi_statements=" + capability.isClientMultiStatements();
    }

    private static Command query(String sql) {
        return command("COM_QUERY " + sql, 0x03, sql);
    }

    private static Command fieldList(String table) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(0x04);
        byte[] tableBytes = table.getBytes(StandardCharsets.UTF_8);
        payload.write(tableBytes, 0, tableBytes.length);
        payload.write(0);
        return new Command("COM_FIELD_LIST " + table, payload.toByteArray());
    }

    private static Command stmtClose(int stmtId) {
        ByteBuffer payload = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
        payload.put((byte) 0x19);
        payload.putInt(stmtId);
        return new Command("COM_STMT_CLOSE " + stmtId, payload.array());
    }

    private static Command setOption(int option) {
        ByteBuffer payload = ByteBuffer.allocate(3).order(ByteOrder.LITTLE_ENDIAN);
        payload.put((byte) 0x1B);
        payload.putShort((short) option);
        return new Command("COM_SET_OPTION " + option, payload.array());
    }

    private static Command command(String label, int commandCode, String argument) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(commandCode);
        byte[] argumentBytes = argument.getBytes(StandardCharsets.UTF_8);
        payload.write(argumentBytes, 0, argumentBytes.length);
        return new Command(label, payload.toByteArray());
    }

    private static class Command {
        private final String label;
        private final byte[] payload;

        Command(String label, byte[] payload) {
            this.label = label;
            this.payload = payload;
        }
    }

    private static class GoldenCase {
        private final String name;
        private final int clientFlags;
        private final ProtocolGolden.Fidelity fidelity;
        private final List<Command> commands = Lists.newArrayList();

        GoldenCase(String name, int clientFlags) {
            this(name, clientFlags, ProtocolGolden.Fidelity.BYTES);
        }

        GoldenCase(String name, int clientFlags, ProtocolGolden.Fidelity fidelity) {
            this.name = name;
            this.clientFlags = clientFlags;
            this.fidelity = fidelity;
        }

        GoldenCase add(Command command) {
            commands.add(command);
            return this;
        }
    }
}
