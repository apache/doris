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
import org.apache.doris.catalog.Env;
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
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ResultSet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
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
 * {@code UserException} (so the command shell adds the legacy "Failed to execute action:" prefix), and that
 * the legacy {@code IcebergExternalTable} routing is untouched (dormant pre-cutover).
 */
public class ConnectorExecuteActionTest {

    private static final String CATALOG = "test_catalog";
    private static final String REMOTE_DB = "remote_db";
    private static final String REMOTE_TBL = "remote_tbl";

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
    public void executeRejectsWhereConditionUntilLoweringLands() {
        Fixture f = new Fixture();
        Expression where = Mockito.mock(Expression.class);
        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.of(where), f.table);

        DdlException e = Assertions.assertThrows(DdlException.class, () -> action.execute(f.table));
        Assertions.assertTrue(e.getMessage().contains("WHERE"),
                "A present WHERE must be rejected (lowering deferred to the rewrite_data_files write-path RFC)");
        // The connector must never be reached when WHERE is present.
        Mockito.verifyNoInteractions(f.procedureOps);
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

    // -------- validate(): engine keeps the ALTER privilege check --------

    @Test
    public void validateEnforcesAlterPrivilege() {
        Fixture f = new Fixture();
        TableNameInfo tableName = new TableNameInfo(CATALOG, "db", "tbl");
        UserIdentity user = Mockito.mock(UserIdentity.class);
        Mockito.when(user.getQualifiedUser()).thenReturn("u");

        ConnectorExecuteAction action = new ConnectorExecuteAction("rollback_to_snapshot",
                f.props, Optional.empty(), Optional.empty(), f.table);

        Env env = Mockito.mock(Env.class);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getRemoteIP()).thenReturn("127.0.0.1");
        // ErrorReport.reportCommon records the error on ctx.getState() before throwing the AnalysisException.
        Mockito.when(ctx.getState()).thenReturn(Mockito.mock(QueryState.class));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
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

    // -------- legacy routing must be untouched (dormant pre-cutover) --------

    @Test
    public void createActionStillRoutesIcebergExternalTableToLegacyFactory() throws Exception {
        IcebergExternalTable legacyTable = Mockito.mock(IcebergExternalTable.class);
        ExecuteAction action = ExecuteActionFactory.createAction("rollback_to_snapshot",
                new HashMap<>(), Optional.empty(), Optional.empty(), legacyTable);
        Assertions.assertFalse(action instanceof ConnectorExecuteAction,
                "An IcebergExternalTable must still route to the legacy fe-core action factory (P6.7 removes it)");
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
        final PluginDrivenExternalTable table;

        Fixture() {
            props.put("snapshot_id", "200");
            Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
            Mockito.when(connector.getProcedureOps()).thenReturn(procedureOps);
            Mockito.when(metadata.getTableHandle(Mockito.any(ConnectorSession.class),
                    Mockito.anyString(), Mockito.anyString())).thenReturn(Optional.of(handle));

            TestPluginCatalog catalog = new TestPluginCatalog(connector, session);
            @SuppressWarnings("unchecked")
            ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
            Mockito.when(db.getRemoteName()).thenReturn(REMOTE_DB);
            this.table = new PluginDrivenExternalTable(1L, "local_tbl", REMOTE_TBL, catalog, db);
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
