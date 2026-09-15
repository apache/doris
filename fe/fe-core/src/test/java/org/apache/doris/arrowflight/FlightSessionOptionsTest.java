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

package org.apache.doris.arrowflight;

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.SessionOptionValue;
import org.apache.arrow.flight.SessionOptionValueFactory;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.SetSessionOptionsResult.ErrorValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A session option of an Arrow Flight SQL session stands for a statement of that session --
 * {@code SWITCH}, {@code USE}, {@code SET SESSION} -- and setting it runs that statement, so these
 * run the real ones on an in-process frontend: what is set, what is refused and as what, and what
 * reads back.
 */
public class FlightSessionOptionsTest extends TestWithFeService {
    private static final String DB = "flight_session_options_db";
    private static final String OTHER_DB = "flight_session_options_other_db";
    private static final String USER = "flight_session_options_user";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB);
        createDatabase(OTHER_DB);
        // A user with no privilege at all: what a statement refuses for lack of privilege is ERROR,
        // whatever the value.
        addUser(USER, true);
    }

    private static SessionOptionValue str(String value) {
        return SessionOptionValueFactory.makeSessionOptionValue(value);
    }

    private ConnectContext flightSession(UserIdentity user) {
        ConnectContext ctx = ConnectContext.forFlight("flight-session-options-peer");
        ctx.setCurrentUserIdentity(user);
        ctx.setRemoteIP("127.0.0.1");
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    private ConnectContext rootSession() {
        return flightSession(UserIdentity.ROOT);
    }

    // Sets one option the way the producer does, as a command of the session, and leaves the
    // test's own context on the thread afterwards.
    private ErrorValue set(ConnectContext ctx, String name, SessionOptionValue value) {
        try {
            return FlightSessionOptions.setOne(ctx, name, value);
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    private Map<String, SetSessionOptionsResult.Error> setAll(ConnectContext ctx,
            Map<String, SessionOptionValue> options) {
        try {
            return FlightSessionOptions.set(ctx, options);
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    private static String get(ConnectContext ctx, String name) {
        SessionOptionValue value = FlightSessionOptions.get(ctx).get(name);
        Assertions.assertNotNull(value, "option " + name + " is not reported");
        return value.acceptVisitor(new org.apache.arrow.flight.NoOpSessionOptionValueVisitor<String>() {
            @Override
            public String visit(String v) {
                return v;
            }
        });
    }

    @Test
    public void testCatalogAndSchemaAreTheCurrentCatalogAndDatabase() {
        ConnectContext ctx = rootSession();
        Assertions.assertEquals("internal", get(ctx, FlightSessionOptions.CATALOG));
        Assertions.assertEquals("", get(ctx, FlightSessionOptions.SCHEMA), "no database chosen yet");

        Assertions.assertNull(set(ctx, FlightSessionOptions.SCHEMA, str(DB)));
        Assertions.assertEquals(DB, ctx.getDatabase());
        Assertions.assertEquals(DB, get(ctx, FlightSessionOptions.SCHEMA));

        Assertions.assertNull(set(ctx, FlightSessionOptions.CATALOG, str("internal")));
        Assertions.assertEquals("internal", ctx.getDefaultCatalog());
        // Switching to the catalog the session is already in keeps its database.
        Assertions.assertEquals(DB, get(ctx, FlightSessionOptions.SCHEMA));
    }

    @Test
    public void testUnknownCatalogOrDatabaseIsAnInvalidValue() {
        ConnectContext ctx = rootSession();
        Assertions.assertEquals(ErrorValue.INVALID_VALUE, set(ctx, FlightSessionOptions.CATALOG, str("no_such_catalog")));
        Assertions.assertEquals("internal", ctx.getDefaultCatalog());
        // A name no catalog can have is the value's fault as much as a name no catalog has.
        Assertions.assertEquals(ErrorValue.INVALID_VALUE, set(ctx, FlightSessionOptions.CATALOG, str("not.a.catalog.name")));
        Assertions.assertEquals("internal", ctx.getDefaultCatalog());
        Assertions.assertEquals(ErrorValue.INVALID_VALUE, set(ctx, FlightSessionOptions.SCHEMA, str("no_such_db")));
        Assertions.assertEquals("", ctx.getDatabase());
        // A catalog or a database is named by a string, and by nothing else.
        Assertions.assertEquals(ErrorValue.INVALID_VALUE,
                set(ctx, FlightSessionOptions.SCHEMA, SessionOptionValueFactory.makeSessionOptionValue(true)));
        Assertions.assertEquals(ErrorValue.INVALID_VALUE, set(ctx, FlightSessionOptions.CATALOG,
                SessionOptionValueFactory.makeSessionOptionValue(new String[] {"internal"})));
        // A session is always in some catalog: there is none the empty value or string could name.
        Assertions.assertEquals(ErrorValue.INVALID_VALUE, set(ctx, FlightSessionOptions.CATALOG, str("")));
        Assertions.assertEquals(ErrorValue.INVALID_VALUE,
                set(ctx, FlightSessionOptions.CATALOG, SessionOptionValueFactory.makeEmptySessionOptionValue()));
        Assertions.assertEquals("internal", ctx.getDefaultCatalog());
    }

    // What GetSessionOptions reads back can be set back, the no-database state included: the empty
    // value (Flight's way of unsetting an option; what the ADBC driver sends to erase one) and the
    // empty string GET reports for it both put the session back into no database.
    @Test
    public void testTheEmptyValueOrStringLeavesTheDatabase() {
        ConnectContext ctx = rootSession();
        Assertions.assertNull(set(ctx, FlightSessionOptions.SCHEMA, str(DB)));
        Assertions.assertEquals(DB, ctx.getDatabase());
        Assertions.assertNull(set(ctx, FlightSessionOptions.SCHEMA, SessionOptionValueFactory.makeEmptySessionOptionValue()));
        Assertions.assertEquals("", ctx.getDatabase());
        Assertions.assertEquals("", get(ctx, FlightSessionOptions.SCHEMA));
        Assertions.assertEquals(-1, ctx.getCurrentDbId());

        // The round trip: what GET answered for a fresh session, set back after a database was chosen.
        String initial = get(rootSession(), FlightSessionOptions.SCHEMA);
        Assertions.assertEquals("", initial);
        Assertions.assertNull(set(ctx, FlightSessionOptions.SCHEMA, str(DB)));
        Assertions.assertNull(set(ctx, FlightSessionOptions.SCHEMA, str(initial)));
        Assertions.assertEquals("", ctx.getDatabase());
        Assertions.assertEquals("", get(ctx, FlightSessionOptions.SCHEMA));
        // Leaving a database the session is not in is nothing, not an error; and a user without any
        // privilege may leave as well: nothing is entered.
        Assertions.assertNull(set(ctx, FlightSessionOptions.SCHEMA, str("")));
        Assertions.assertNull(set(flightSession(UserIdentity.createAnalyzedUserIdentWithIp(USER, "%")),
                FlightSessionOptions.SCHEMA, SessionOptionValueFactory.makeEmptySessionOptionValue()));
    }

    @Test
    public void testWhatTheStatementRefusesForLackOfPrivilegeIsAnError() throws Exception {
        ConnectContext ctx = flightSession(UserIdentity.createAnalyzedUserIdentWithIp(USER, "%"));
        Assertions.assertEquals(ErrorValue.ERROR, set(ctx, FlightSessionOptions.SCHEMA, str(DB)));
        Assertions.assertEquals("", ctx.getDatabase());
        // The statement checks the privilege before it looks the name up, and so does the option:
        // a user is not told whether a catalog they may not see exists. The name's format, which
        // says nothing about what exists, is checked before either.
        Assertions.assertEquals(ErrorValue.ERROR, set(ctx, FlightSessionOptions.CATALOG, str("no_such_catalog")));
        Assertions.assertEquals(ErrorValue.INVALID_VALUE, set(ctx, FlightSessionOptions.CATALOG, str("not.a.catalog.name")));
    }

    @Test
    public void testASessionVariableTakesAValueOfItsTypeOrAString() {
        ConnectContext ctx = rootSession();
        Assertions.assertNull(set(ctx, "query_timeout", SessionOptionValueFactory.makeSessionOptionValue(77L)));
        Assertions.assertEquals(77, ctx.getSessionVariable().getQueryTimeoutS());
        Assertions.assertEquals("77", get(ctx, "query_timeout"));

        // *DBC drivers only have strings; SET converts them like it does for any client.
        Assertions.assertNull(set(ctx, "query_timeout", str("88")));
        Assertions.assertEquals(88, ctx.getSessionVariable().getQueryTimeoutS());

        Assertions.assertNull(set(ctx, "enable_profile", SessionOptionValueFactory.makeSessionOptionValue(true)));
        Assertions.assertTrue(ctx.getSessionVariable().enableProfile());
        Assertions.assertEquals("true", get(ctx, "enable_profile"));
        Assertions.assertNull(set(ctx, "enable_profile", str("false")));
        Assertions.assertFalse(ctx.getSessionVariable().enableProfile());

        Assertions.assertNull(set(ctx, "insert_max_filter_ratio", SessionOptionValueFactory.makeSessionOptionValue(0.25)));
        Assertions.assertEquals(0.25, ctx.getSessionVariable().getInsertMaxFilterRatio());
        Assertions.assertEquals("0.25", get(ctx, "insert_max_filter_ratio"));
    }

    @Test
    public void testTheEmptyValueSetsTheVariableBackToItsDefault() {
        ConnectContext ctx = rootSession();
        int defaultTimeout = VariableMgr.getDefaultSessionVariable().getQueryTimeoutS();
        Assertions.assertNull(set(ctx, "query_timeout", SessionOptionValueFactory.makeSessionOptionValue(defaultTimeout + 5L)));
        Assertions.assertEquals(defaultTimeout + 5, ctx.getSessionVariable().getQueryTimeoutS());
        Assertions.assertNull(set(ctx, "query_timeout", SessionOptionValueFactory.makeEmptySessionOptionValue()));
        Assertions.assertEquals(defaultTimeout, ctx.getSessionVariable().getQueryTimeoutS());
    }

    @Test
    public void testAStringIsQuotedForTheSessionsSqlMode() {
        ConnectContext ctx = rootSession();
        String text = "it's \"quoted\" and back\\slashed \\n but not escaped";
        Assertions.assertNull(set(ctx, "session_context", str(text)));
        Assertions.assertEquals(text, ctx.getSessionVariable().sessionContext);
        Assertions.assertEquals(text, get(ctx, "session_context"));

        // Under NO_BACKSLASH_ESCAPES a backslash in a literal is itself; the option is quoted for the
        // mode the statement is parsed under, so the value still arrives as sent.
        long sqlMode = ctx.getSessionVariable().getSqlMode();
        ctx.getSessionVariable().setSqlMode(sqlMode | SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
        try {
            Assertions.assertNull(set(ctx, "session_context", str(text)));
            Assertions.assertEquals(text, ctx.getSessionVariable().sessionContext);
        } finally {
            ctx.getSessionVariable().setSqlMode(sqlMode);
        }
    }

    @Test
    public void testANameThatIsNoSessionVariableIsAnInvalidName() {
        ConnectContext ctx = rootSession();
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "no_such_variable", str("1")));
        // The ADBC connection option names are not what the driver puts on the wire.
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "adbc.connection.catalog", str("internal")));
        // What is not an identifier never reaches the parser.
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "@user_var", str("1")));
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "query_timeout = 1; select 1", str("1")));
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "", str("1")));
        // A variable SET ignores for the sake of old MySQL clients is no variable of this session.
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "use_v2_rollup", str("true")));
    }

    // The names that can be set are the names that are read back, spelled the same: a client looks
    // an option up in what GetSessionOptions answers by the name it set it under. SET takes more
    // spellings and more names; those are no options.
    @Test
    public void testAVariableIsNamedAsItIsReadBackAndOnlySo() {
        ConnectContext ctx = rootSession();
        Map<String, SessionOptionValue> options = FlightSessionOptions.get(ctx);

        // Not in another case, although SET takes any.
        Assertions.assertTrue(options.containsKey("query_timeout"));
        Assertions.assertNotNull(VariableMgr.getVarContext("Query_Timeout"));
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "Query_Timeout", str("1")));

        // An experimental variable is read back with its prefix, so it is set with it; SET takes the
        // name without.
        Assertions.assertFalse(options.containsKey("enable_shared_scan"));
        Assertions.assertTrue(options.containsKey("experimental_enable_shared_scan"));
        Assertions.assertNotNull(VariableMgr.getVarContext("enable_shared_scan"));
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "enable_shared_scan", str("true")));
        Assertions.assertNull(set(ctx, "experimental_enable_shared_scan", str("true")));
        Assertions.assertEquals("true", get(ctx, "experimental_enable_shared_scan"));
        // One that went GA is read back without the prefix; SET still takes it with, for old scripts.
        Assertions.assertTrue(options.containsKey("enable_bucket_shuffle_join"));
        Assertions.assertNotNull(VariableMgr.getVarContext("experimental_enable_bucket_shuffle_join"));
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "experimental_enable_bucket_shuffle_join", str("true")));
        Assertions.assertNull(set(ctx, "enable_bucket_shuffle_join", str("true")));

        // A variable SET keeps for old clients although it does nothing any more, and one SHOW
        // VARIABLES hides, are not read back, so they are no options.
        Assertions.assertFalse(options.containsKey("enable_nereids_dml"));
        Assertions.assertNotNull(VariableMgr.getVarContext("enable_nereids_dml"));
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "enable_nereids_dml", str("true")));
        Assertions.assertFalse(options.containsKey("deprecated_enable_local_exchange"));
        Assertions.assertNotNull(VariableMgr.getVarContext("deprecated_enable_local_exchange"));
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "deprecated_enable_local_exchange", str("true")));
        Assertions.assertEquals(ErrorValue.INVALID_NAME, set(ctx, "enable_local_exchange", str("true")));

        // And every name read back is one the session knows as an option.
        for (String name : options.keySet()) {
            if (!name.equals(FlightSessionOptions.CATALOG) && !name.equals(FlightSessionOptions.SCHEMA)) {
                Assertions.assertNotNull(VariableMgr.getShownVarContext(name), name);
            }
        }
    }

    @Test
    public void testAVariableThatCannotBeSetForTheSessionIsAnError() {
        ConnectContext ctx = rootSession();
        // Read-only.
        Assertions.assertEquals(ErrorValue.ERROR, set(ctx, "net_buffer_length", SessionOptionValueFactory.makeSessionOptionValue(1L)));
        // One instance per frontend: SET GLOBAL sets it, a session option cannot.
        Assertions.assertEquals(ErrorValue.ERROR, set(ctx, "analyze_timeout", SessionOptionValueFactory.makeSessionOptionValue(1L)));
    }

    @Test
    public void testAValueTheVariableRefusesIsAnInvalidValue() {
        ConnectContext ctx = rootSession();
        int before = ctx.getSessionVariable().getQueryTimeoutS();
        Assertions.assertEquals(ErrorValue.INVALID_VALUE, set(ctx, "query_timeout", str("not a number")));
        Assertions.assertEquals(before, ctx.getSessionVariable().getQueryTimeoutS());
        // What the statement's own validation refuses.
        Assertions.assertEquals(ErrorValue.INVALID_VALUE, set(ctx, "time_zone", str("Mars/Olympus_Mons")));
        // What has no literal at all.
        Assertions.assertEquals(ErrorValue.INVALID_VALUE,
                set(ctx, "query_timeout", SessionOptionValueFactory.makeSessionOptionValue(new String[] {"1", "2"})));
        Assertions.assertEquals(ErrorValue.INVALID_VALUE,
                set(ctx, "insert_max_filter_ratio", SessionOptionValueFactory.makeSessionOptionValue(Double.NaN)));
    }

    @Test
    public void testARequestSetsEachOptionOnItsOwnAndReportsTheOnesItCouldNot() {
        ConnectContext ctx = rootSession();
        Map<String, SetSessionOptionsResult.Error> errors = setAll(ctx, ImmutableMap.of(
                "query_timeout", SessionOptionValueFactory.makeSessionOptionValue(66L),
                "no_such_variable", str("1"),
                FlightSessionOptions.SCHEMA, str(OTHER_DB),
                "net_buffer_length", str("1"),
                "time_zone", str("Mars/Olympus_Mons")));
        Assertions.assertEquals(ImmutableMap.of(
                "no_such_variable", new SetSessionOptionsResult.Error(ErrorValue.INVALID_NAME),
                "net_buffer_length", new SetSessionOptionsResult.Error(ErrorValue.ERROR),
                "time_zone", new SetSessionOptionsResult.Error(ErrorValue.INVALID_VALUE)), errors);
        Assertions.assertEquals(66, ctx.getSessionVariable().getQueryTimeoutS());
        Assertions.assertEquals(OTHER_DB, ctx.getDatabase());
        Assertions.assertTrue(setAll(ctx, ImmutableMap.of()).isEmpty());
    }

    // Setting or reading options is no request of the session: what the last request left -- the
    // result it cached on this frontend, the query whose coordinator it keeps alive for the client's
    // DoGet -- stays where it is, whatever statements the options run.
    @Test
    public void testTheOptionsLeaveTheLastRequestAlone() throws Exception {
        ConnectContext ctx = rootSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        ctx.setQueryId(new TUniqueId(1, 1));
        ctx.getResultSender().sendResultSet(new ShowResultSet(
                ShowResultSetMetaData.builder().addColumn(new Column("c", ScalarType.createVarchar(20))).build(),
                Lists.<List<String>>newArrayList(Lists.newArrayList("v"))), null, false);
        StmtExecutor deferred = Mockito.mock(StmtExecutor.class);
        ctx.addFlightSqlDeferredExecutor(deferred);
        Assertions.assertEquals(1, adapter.getChannel().resultNum());

        Assertions.assertNull(set(ctx, "query_timeout", SessionOptionValueFactory.makeSessionOptionValue(77L)));
        Assertions.assertNull(set(ctx, FlightSessionOptions.SCHEMA, str(DB)));
        Assertions.assertEquals(ErrorValue.INVALID_VALUE, set(ctx, "query_timeout", str("not a number")));
        Assertions.assertEquals("77", get(ctx, "query_timeout"));
        // The statements ran under query ids of their own ...
        Assertions.assertNotEquals(new TUniqueId(1, 1), ctx.queryId());
        // ... and left the last request's result and query as they were.
        Assertions.assertEquals(1, adapter.getChannel().resultNum());
        Assertions.assertNotNull(adapter.getChannel().getResult(DebugUtil.printId(new TUniqueId(1, 1))));
        Mockito.verify(deferred, Mockito.never()).finalizeArrowFlightQuery();
        ctx.closeFlightSqlDeferredExecutors();
        Mockito.verify(deferred).finalizeArrowFlightQuery();
    }

    @Test
    public void testTheOptionsReadBackAreShowVariablesAsText() {
        ConnectContext ctx = rootSession();
        Assertions.assertNull(set(ctx, "query_timeout", SessionOptionValueFactory.makeSessionOptionValue(55L)));
        Map<String, SessionOptionValue> options = FlightSessionOptions.get(ctx);
        List<List<String>> shown = VariableMgr.dump(SetType.SESSION, ctx.getSessionVariable(), null);
        Assertions.assertEquals(shown.size() + 2, options.size());
        for (List<String> row : shown) {
            Assertions.assertEquals(str(row.get(1)), options.get(row.get(0)), row.get(0));
        }
        Assertions.assertEquals(str("55"), options.get("query_timeout"));
        Assertions.assertEquals(shown.stream().map(row -> row.get(0)).collect(Collectors.toSet()),
                options.keySet().stream().filter(name -> !name.equals(FlightSessionOptions.CATALOG)
                        && !name.equals(FlightSessionOptions.SCHEMA)).collect(Collectors.toSet()));
    }
}
