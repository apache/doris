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

package org.apache.doris.nereids.trees.plans.commands.execute;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.procedure.ProcedureExecutionMode;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link ConnectorExecuteAction} and the {@code PluginDrivenExternalTable} branch of
 * {@link ExecuteActionFactory} (P6.4-T07 dispatch rewire).
 *
 * <p><b>WHY this matters:</b> at the P6.6 iceberg cutover, {@code ALTER TABLE t EXECUTE proc(...)} on an
 * iceberg table (then a {@code PluginDrivenExternalTable}) must route through the connector's
 * {@link ConnectorProcedureOps} instead of the legacy fe-core actions, while the engine keeps the
 * {@code ALTER} privilege check, the single-row {@code CommonResultSet} wrapping and the edit-log refresh
 * (D-062 §2). These tests pin that the dispatch threads the catalog's session/handle into
 * {@code getProcedureOps().execute(...)}, wraps the engine-neutral {@link ConnectorProcedureResult} back
 * into a {@code ResultSet}, surfaces the connector's {@link DorisConnectorException} as a
 * {@code UserException} (so the command shell adds the legacy "Failed to execute action:" prefix).
 */
public class ConnectorExecuteActionTest {

    private static final String CATALOG = "test_catalog";
    private static final String REMOTE_DB = "remote_db";
    private static final String REMOTE_TBL = "remote_tbl";

    // execute() now refreshes the table's caches after a successful commit (H-6) via
    // Env.getCurrentEnv().getRefreshManager().refreshTableInternal(...). Stub that statically for every test so
    // the dispatch paths don't NPE, and so the refresh can be verified.
    private MockedStatic<Env> envStatic;
    private Env env;
    private RefreshManager refreshManager;

    @BeforeEach
    public void setUpEnv() {
        envStatic = Mockito.mockStatic(Env.class);
        env = Mockito.mock(Env.class);
        refreshManager = Mockito.mock(RefreshManager.class);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
    }

    @AfterEach
    public void tearDownEnv() {
        envStatic.close();
    }

    // -------- ExecuteActionFactory routing --------

    @Test
    public void createActionRoutesPluginDrivenTableToConnectorAdapter() throws Exception {
        Fixture f = new Fixture();
        ExecuteAction action = ExecuteActionFactory.createAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);
        Assertions.assertTrue(action instanceof ConnectorExecuteAction,
                "A PluginDrivenExternalTable must dispatch to the connector procedure adapter, not a legacy action");
    }

    @Test
    public void getSupportedActionsReturnsConnectorProcedureNames() {
        Fixture f = new Fixture();
        Mockito.when(f.procedureOps.getSupportedProcedures())
                .thenReturn(Arrays.asList("rollback_to_snapshot", "expire_snapshots"));
        String[] actions = ExecuteActionFactory.getSupportedActions(f.table);
        Assertions.assertArrayEquals(new String[] {"rollback_to_snapshot", "expire_snapshots"}, actions,
                "getSupportedActions must export the connector's supported procedure names");
    }

    // -------- execute(): dispatch + result wrapping --------

    @Test
    public void executeThreadsSessionAndHandleIntoConnectorAndWrapsResult() throws Exception {
        Fixture f = new Fixture();
        Mockito.when(f.procedureOps.execute(Mockito.any(), Mockito.any(), Mockito.anyString(),
                        Mockito.anyMap(), Mockito.any(), Mockito.anyList()))
                .thenReturn(twoColumnResult(Arrays.asList("100", "200")));

        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);
        ResultSet rs = action.execute(f.table);

        // The connector saw the catalog's session + the resolved handle + the engine-neutral carriers.
        Mockito.verify(f.procedureOps).execute(Mockito.eq(f.session), Mockito.eq(f.handle),
                Mockito.eq("rollback_to_snapshot"), Mockito.eq(f.props),
                Mockito.isNull(), Mockito.eq(Collections.emptyList()));

        // The ConnectorProcedureResult was converted into a CommonResultSet (columns via ConnectorColumnConverter).
        List<String> colNames = Arrays.asList(rs.getMetaData().getColumns().get(0).getName(),
                rs.getMetaData().getColumns().get(1).getName());
        Assertions.assertEquals(Arrays.asList("previous_snapshot_id", "current_snapshot_id"), colNames);
        Assertions.assertEquals(Collections.singletonList(Arrays.asList("100", "200")), rs.getResultRows());

        // The converted Doris column metadata (type + nullability) is client-visible, so pin it too: twoColumnResult
        // builds BIGINT non-null columns, mirroring IcebergRollbackToSnapshotAction.getResultSchema's
        // (Type.BIGINT, false) columns — a ConnectorColumnConverter mutation that drops the type/nullability is caught.
        Assertions.assertEquals(org.apache.doris.catalog.Type.BIGINT, rs.getMetaData().getColumns().get(0).getType());
        Assertions.assertEquals(org.apache.doris.catalog.Type.BIGINT, rs.getMetaData().getColumns().get(1).getType());
        Assertions.assertFalse(rs.getMetaData().getColumns().get(0).isAllowNull());
        Assertions.assertFalse(rs.getMetaData().getColumns().get(1).isAllowNull());
    }

    @Test
    public void wrapResultRoundTripsColumnNullabilityBothPolarities() throws Exception {
        // A fast_forward-SHAPED schema mixes a non-nullable column with a nullable one. The converter must
        // round-trip BOTH polarities through ConnectorColumn.isNullable() -> Column(isAllowNull), so a converter
        // mutation that drops (or forces) nullability is caught — not just the all-non-null twoColumnResult case.
        Fixture f = new Fixture();
        List<ConnectorColumn> schema = Arrays.asList(
                new ConnectorColumn("branch_updated", ConnectorType.of("STRING"), "c", false, null),
                new ConnectorColumn("previous_ref", ConnectorType.of("BIGINT"), "c", true, null),
                new ConnectorColumn("updated_ref", ConnectorType.of("BIGINT"), "c", false, null));
        Mockito.when(f.procedureOps.execute(Mockito.any(), Mockito.any(), Mockito.anyString(),
                        Mockito.anyMap(), Mockito.any(), Mockito.anyList()))
                .thenReturn(new ConnectorProcedureResult(schema,
                        Collections.singletonList(Arrays.asList("main", "100", "200"))));
        ConnectorExecuteAction action = new ConnectorExecuteAction("fast_forward",
                f.props, Optional.empty(), Optional.empty(), f.table);
        ResultSet rs = action.execute(f.table);
        Assertions.assertFalse(rs.getMetaData().getColumns().get(0).isAllowNull());
        Assertions.assertTrue(rs.getMetaData().getColumns().get(1).isAllowNull(),
                "the nullable column must round-trip nullable through ConnectorColumnConverter");
        Assertions.assertFalse(rs.getMetaData().getColumns().get(2).isAllowNull());
        Assertions.assertEquals(org.apache.doris.catalog.Type.BIGINT,
                rs.getMetaData().getColumns().get(1).getType());
    }

    @Test
    public void executePassesPartitionNamesThrough() throws Exception {
        Fixture f = new Fixture();
        Mockito.when(f.procedureOps.execute(Mockito.any(), Mockito.any(), Mockito.anyString(),
                        Mockito.anyMap(), Mockito.any(), Mockito.anyList()))
                .thenReturn(twoColumnResult(Arrays.asList("1", "2")));

        PartitionNamesInfo partitions = new PartitionNamesInfo(false, Arrays.asList("p1", "p2"));
        ConnectorExecuteAction action = new ConnectorExecuteAction("expire_snapshots",
                f.props, Optional.of(partitions), Optional.empty(), f.table);
        action.execute(f.table);

        Mockito.verify(f.procedureOps).execute(Mockito.any(), Mockito.any(), Mockito.eq("expire_snapshots"),
                Mockito.anyMap(), Mockito.isNull(), Mockito.eq(Arrays.asList("p1", "p2")));
    }

    @Test
    public void executeSingleCallActionRejectsWhereCondition() {
        // The eight pure-SDK procedures are SINGLE_CALL and never accept a WHERE; it must be rejected fail-loud
        // (only a DISTRIBUTED rewrite scopes its work by a WHERE). A bare mock Expression is enough — the reject
        // happens on the mode check, before any lowering.
        Fixture f = new Fixture();
        Mockito.when(f.procedureOps.getExecutionMode("rollback_to_snapshot"))
                .thenReturn(ProcedureExecutionMode.SINGLE_CALL);
        Expression where = Mockito.mock(Expression.class);
        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.of(where), f.table);

        DdlException e = Assertions.assertThrows(DdlException.class, () -> action.execute(f.table));
        Assertions.assertTrue(e.getMessage().contains("WHERE"),
                "a SINGLE_CALL procedure must reject a WHERE (only DISTRIBUTED rewrite accepts one)");
        // The connector body must never run for a rejected WHERE.
        Mockito.verify(f.procedureOps, Mockito.never()).execute(Mockito.any(), Mockito.any(),
                Mockito.anyString(), Mockito.anyMap(), Mockito.any(), Mockito.anyList());
        Mockito.verify(f.procedureOps, Mockito.never()).planRewrite(Mockito.any(), Mockito.any(),
                Mockito.anyString(), Mockito.anyMap(), Mockito.any(), Mockito.anyList());
    }

    @Test
    public void executeDistributedActionLowersWhereAndThreadsItToPlanRewrite() throws Exception {
        // The DISTRIBUTED arm lowers the (unbound) WHERE to a neutral ConnectorPredicate and threads it to the
        // connector's planRewrite — it must NOT reject it, and it must NOT pass null. Stub planRewrite to return
        // no groups so the driver returns the all-zero row without opening a transaction.
        Fixture f = new Fixture();
        Mockito.when(f.procedureOps.getExecutionMode("rewrite_data_files"))
                .thenReturn(ProcedureExecutionMode.DISTRIBUTED);
        Mockito.when(f.procedureOps.planRewrite(Mockito.any(), Mockito.any(), Mockito.anyString(),
                        Mockito.anyMap(), Mockito.any(), Mockito.anyList()))
                .thenReturn(Collections.emptyList());

        Expression where = new GreaterThan(new UnboundSlot("a"), new IntegerLiteral(5));
        ConnectorExecuteAction action = new ConnectorExecuteAction("rewrite_data_files",
                f.props, Optional.empty(), Optional.of(where), f.table);
        ResultSet rs = action.execute(f.table);

        ArgumentCaptor<ConnectorPredicate> captor = ArgumentCaptor.forClass(ConnectorPredicate.class);
        Mockito.verify(f.procedureOps).planRewrite(Mockito.any(), Mockito.any(),
                Mockito.eq("rewrite_data_files"), Mockito.anyMap(), captor.capture(), Mockito.anyList());
        ConnectorPredicate lowered = captor.getValue();
        Assertions.assertNotNull(lowered,
                "the DISTRIBUTED arm must lower the WHERE and pass a non-null predicate, not drop it to null");
        Assertions.assertInstanceOf(ConnectorComparison.class, lowered.getExpression());
        Assertions.assertEquals("a", ((ConnectorColumnRef) ((ConnectorComparison) lowered.getExpression())
                .getLeft()).getColumnName());
        // No groups -> the legacy all-zero four-column rewrite result row.
        Assertions.assertEquals(Collections.singletonList(Arrays.asList("0", "0", "0", "0")), rs.getResultRows());
    }

    @Test
    public void executeReWrapsConnectorExceptionAsUserExceptionWithSameMessage() {
        Fixture f = new Fixture();
        Mockito.when(f.procedureOps.execute(Mockito.any(), Mockito.any(), Mockito.anyString(),
                        Mockito.anyMap(), Mockito.any(), Mockito.anyList()))
                .thenThrow(new DorisConnectorException("Snapshot 7 not found in table remote_tbl"));

        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);

        // Must surface as a checked UserException so ExecuteActionCommand.run() catches it and re-wraps it with
        // the legacy "Failed to execute action:" prefix. The exact plain UserException type matches what the
        // legacy action bodies threw, and the inner message is preserved verbatim (T08 byte-parity).
        UserException e = Assertions.assertThrows(
                UserException.class, () -> action.execute(f.table));
        Assertions.assertEquals(UserException.class, e.getClass(),
                "Re-wrap must use the plain UserException type the legacy action body threw (no extra errCode layer)");
        Assertions.assertEquals("Snapshot 7 not found in table remote_tbl", e.getDetailMessage());
    }

    @Test
    public void executeThrowsWhenConnectorExposesNoProcedureOps() {
        Fixture f = new Fixture();
        Mockito.when(f.connector.getProcedureOps()).thenReturn(null);

        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);
        DdlException e = Assertions.assertThrows(DdlException.class, () -> action.execute(f.table));
        Assertions.assertTrue(e.getMessage().contains("does not support"),
                "A connector with no procedure ops must fail loud, mirroring the write-path null-provider throw");
    }

    @Test
    public void executeThrowsWhenTableHandleMissing() {
        Fixture f = new Fixture();
        Mockito.when(f.metadata.getTableHandle(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(Optional.empty());

        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class, () -> action.execute(f.table));
        Assertions.assertTrue(e.getMessage().contains("Table not found"));
    }

    @Test
    public void executeEnforcesSingleRowWidthInvariant() {
        Fixture f = new Fixture();
        // Two declared columns but a one-wide row -> the single-row contract (BaseExecuteAction:106-108) must trip.
        Mockito.when(f.procedureOps.execute(Mockito.any(), Mockito.any(), Mockito.anyString(),
                        Mockito.anyMap(), Mockito.any(), Mockito.anyList()))
                .thenReturn(twoColumnResult(Collections.singletonList("only-one")));

        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);
        Assertions.assertThrows(IllegalStateException.class, () -> action.execute(f.table));
    }

    @Test
    public void executeReturnsNullResultSetForEmptySchema() throws Exception {
        Fixture f = new Fixture();
        Mockito.when(f.procedureOps.execute(Mockito.any(), Mockito.any(), Mockito.anyString(),
                        Mockito.anyMap(), Mockito.any(), Mockito.anyList()))
                .thenReturn(new ConnectorProcedureResult(Collections.emptyList(), Collections.emptyList()));

        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);
        Assertions.assertNull(action.execute(f.table),
                "A procedure with no result columns wraps to a null ResultSet (mirrors BaseExecuteAction)");
    }

    @Test
    public void executeReturnsNullResultSetWhenConnectorReturnsNoRows() throws Exception {
        Fixture f = new Fixture();
        // The connector encodes a null body row as (schema, emptyRows) (BaseIcebergAction.execute). Legacy
        // BaseExecuteAction.execute returns null when the body row is null, so the adapter must too — otherwise
        // a future procedure that returns a null row would send a 0-row result set instead of nothing.
        List<ConnectorColumn> schema = Arrays.asList(
                new ConnectorColumn("c", ConnectorType.of("BIGINT"), "c", false, null));
        Mockito.when(f.procedureOps.execute(Mockito.any(), Mockito.any(), Mockito.anyString(),
                        Mockito.anyMap(), Mockito.any(), Mockito.anyList()))
                .thenReturn(new ConnectorProcedureResult(schema, Collections.emptyList()));

        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);
        Assertions.assertNull(action.execute(f.table),
                "A non-empty schema with zero rows (connector's null-row encoding) wraps to null, like legacy");
    }

    // -------- execute(): leader-side cache refresh after a successful commit (H-6) --------

    @Test
    public void executeRefreshesTableCachesAfterSuccessfulSingleCall() throws Exception {
        Fixture f = new Fixture();
        Mockito.when(f.procedureOps.execute(Mockito.any(), Mockito.any(), Mockito.anyString(),
                        Mockito.anyMap(), Mockito.any(), Mockito.anyList()))
                .thenReturn(twoColumnResult(Arrays.asList("100", "200")));

        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);
        action.execute(f.table);

        // H-6: the FE that ran the procedure must refresh the mutated table through the standard refresh-table
        // path — the only path that clears BOTH the engine meta cache (LOCAL names) and the connector's own
        // per-table cache (REMOTE names, the iceberg latest-snapshot cache, default TTL 24h). Without this the
        // leader keeps serving the pre-procedure snapshot up to 24h (leader/follower split). MUTATION: dropping
        // the refreshTableCachesAfterMutation() call -> verify fails.
        Mockito.verify(refreshManager).refreshTableInternal(
                Mockito.eq(f.db), Mockito.eq(f.table), Mockito.anyLong());
    }

    @Test
    public void executeRefreshesTableCachesAfterSuccessfulDistributedRewrite() throws Exception {
        // The DISTRIBUTED rewrite also produces a new snapshot, so it must refresh too. No groups -> the driver
        // returns the all-zero row without opening a transaction, but the refresh still runs on a normal return.
        Fixture f = new Fixture();
        Mockito.when(f.procedureOps.getExecutionMode("rewrite_data_files"))
                .thenReturn(ProcedureExecutionMode.DISTRIBUTED);
        Mockito.when(f.procedureOps.planRewrite(Mockito.any(), Mockito.any(), Mockito.anyString(),
                        Mockito.anyMap(), Mockito.any(), Mockito.anyList()))
                .thenReturn(Collections.emptyList());

        Expression where = new GreaterThan(new UnboundSlot("a"), new IntegerLiteral(5));
        ConnectorExecuteAction action = new ConnectorExecuteAction("rewrite_data_files",
                f.props, Optional.empty(), Optional.of(where), f.table);
        action.execute(f.table);

        Mockito.verify(refreshManager).refreshTableInternal(
                Mockito.eq(f.db), Mockito.eq(f.table), Mockito.anyLong());
    }

    @Test
    public void executeDoesNotRefreshWhenProcedureFails() {
        Fixture f = new Fixture();
        Mockito.when(f.procedureOps.execute(Mockito.any(), Mockito.any(), Mockito.anyString(),
                        Mockito.anyMap(), Mockito.any(), Mockito.anyList()))
                .thenThrow(new DorisConnectorException("Snapshot 7 not found in table remote_tbl"));

        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);

        Assertions.assertThrows(UserException.class, () -> action.execute(f.table));
        // A failed procedure committed nothing, so it must not refresh (no spurious cache churn; mirrors follower
        // replay which only runs on a logged success). MUTATION: refreshing unconditionally / in a finally -> the
        // never-verify fails.
        Mockito.verify(refreshManager, Mockito.never())
                .refreshTableInternal(Mockito.any(), Mockito.any(), Mockito.anyLong());
    }

    // -------- validate(): engine keeps the ALTER privilege check --------

    @Test
    public void validateEnforcesAlterPrivilege() {
        Fixture f = new Fixture();
        TableNameInfo tableName = new TableNameInfo(CATALOG, "db", "tbl");
        UserIdentity user = Mockito.mock(UserIdentity.class);
        Mockito.when(user.getQualifiedUser()).thenReturn("u");

        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);

        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        // Reuse the shared Env mock (the @BeforeEach MockedStatic<Env> is already active; a second one would
        // throw "static mocking is already registered"); just wire the access manager onto it.
        Mockito.when(env.getAccessManager()).thenReturn(access);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getRemoteIP()).thenReturn("127.0.0.1");
        // ErrorReport.reportCommon records the error on ctx.getState() before throwing the AnalysisException.
        Mockito.when(ctx.getState()).thenReturn(Mockito.mock(QueryState.class));

        try (MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            ctxStatic.when(ConnectContext::get).thenReturn(ctx);

            // Denied -> validate must throw the same access-denied AnalysisException as legacy
            // BaseExecuteAction.validate (ErrorReport.reportAnalysisException with ERR_TABLEACCESS_DENIED_ERROR),
            // not just any exception — so the assertion fails if the privilege error type/message silently changes.
            Mockito.when(access.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.eq(CATALOG),
                    Mockito.eq("db"), Mockito.eq("tbl"), Mockito.eq(PrivPredicate.ALTER))).thenReturn(false);
            AnalysisException denied = Assertions.assertThrows(AnalysisException.class,
                    () -> action.validate(tableName, user));
            Assertions.assertTrue(denied.getMessage().contains("ALTER") && denied.getMessage().contains("denied"),
                    "Denial must carry the legacy ALTER access-denied message");

            // Granted -> validate returns without touching the connector (per-arg validation is connector-side).
            Mockito.when(access.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.eq(CATALOG),
                    Mockito.eq("db"), Mockito.eq("tbl"), Mockito.eq(PrivPredicate.ALTER))).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> action.validate(tableName, user));
            Mockito.verifyNoInteractions(f.procedureOps);
        }
    }

    // -------- helpers --------

    private static ConnectorProcedureResult twoColumnResult(List<String> row) {
        List<ConnectorColumn> schema = Arrays.asList(
                new ConnectorColumn("previous_snapshot_id", ConnectorType.of("BIGINT"), "prev", false, null),
                new ConnectorColumn("current_snapshot_id", ConnectorType.of("BIGINT"), "cur", false, null));
        return new ConnectorProcedureResult(schema, Collections.singletonList(row));
    }

    /** Wires a PluginDrivenExternalTable over a mock connector chain (session/metadata/handle/procedureOps). */
    private static final class Fixture {
        final Map<String, String> props = new HashMap<>();
        final ConnectorSession session = Mockito.mock(ConnectorSession.class);
        final ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        final ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        final ConnectorProcedureOps procedureOps = Mockito.mock(ConnectorProcedureOps.class);
        final Connector connector = Mockito.mock(Connector.class);
        final ExternalDatabase<PluginDrivenExternalTable> db;
        final PluginDrivenExternalTable table;

        @SuppressWarnings("unchecked")
        Fixture() {
            props.put("snapshot_id", "200");
            Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
            Mockito.when(connector.getProcedureOps()).thenReturn(procedureOps);
            Mockito.when(metadata.getTableHandle(Mockito.any(ConnectorSession.class),
                    Mockito.anyString(), Mockito.anyString())).thenReturn(Optional.of(handle));

            TestPluginCatalog catalog = new TestPluginCatalog(connector, session);
            this.db = Mockito.mock(ExternalDatabase.class);
            Mockito.when(db.getRemoteName()).thenReturn(REMOTE_DB);
            this.table = new SchemaPluginTable(1L, "local_tbl", REMOTE_TBL, catalog, db);
        }
    }

    /** A plugin table with one resolvable column {@code a INT}, so the rewrite WHERE lowering can resolve it. */
    private static final class SchemaPluginTable extends PluginDrivenExternalTable {
        SchemaPluginTable(long id, String name, String remoteName, PluginDrivenExternalCatalog catalog,
                ExternalDatabase<PluginDrivenExternalTable> db) {
            super(id, name, remoteName, catalog, db);
        }

        @Override
        public Column getColumn(String colName) {
            return "a".equalsIgnoreCase(colName) ? new Column("a", ScalarType.INT) : null;
        }
    }

    /** Minimal catalog that returns the test connector/session without standing up Env/CatalogMgr. */
    private static final class TestPluginCatalog extends PluginDrivenExternalCatalog {
        private final Connector connector;
        private final ConnectorSession session;

        TestPluginCatalog(Connector connector, ConnectorSession session) {
            super(1L, CATALOG, null, makeProps(), "", connector);
            this.connector = connector;
            this.session = session;
        }

        @Override
        public String getType() {
            return "iceberg";
        }

        @Override
        public Connector getConnector() {
            return connector;
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return session;
        }

        @Override
        protected List<String> listDatabaseNames() {
            return Collections.emptyList();
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }

        private static Map<String, String> makeProps() {
            Map<String, String> props = new HashMap<>();
            props.put("type", "iceberg");
            return props;
        }
    }
}
