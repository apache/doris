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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenSysExternalTable;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Guards the SCAN-PATH handle resolution in {@link PluginDrivenScanNode#create} (B4 final-review BLOCKER).
 *
 * <p><b>Why this matters:</b> for a connector <b>system</b> table ({@link PluginDrivenSysExternalTable},
 * e.g. {@code tbl$binlog}), the scan node must thread the connector's SYSTEM-table handle — the one
 * carrying {@code sysTableName}/{@code forceJni} — not a plain handle for the base table. If
 * {@code create} resolved the handle with a RAW {@code metadata.getTableHandle(...)} (the base-table
 * handle), the connector's force-JNI flag never reaches the scan plan, so a binlog/audit_log split
 * whose native conversion succeeds would take the NATIVE reader instead of JNI and return silently
 * wrong rows. {@code create} must go through the sys-aware seam
 * {@link PluginDrivenExternalTable#resolveConnectorTableHandle} so the override on the sys table feeds
 * the system handle through.</p>
 *
 * <p>This drives the REAL static {@code create(...)} end-to-end (full {@code FileQueryScanNode}
 * constructor chain — same construction used by {@code IcebergScanNodeTest}) over a real sys table,
 * then reads back the node's resolved {@code currentHandle} and asserts it is the SYS handle (distinct
 * mock), not the base handle. fe-core has no paimon on its classpath, so the connector-specific
 * {@code PaimonTableHandle.isForceJni()} cannot be referenced here; asserting "the sys handle, not the
 * base handle, was threaded" is the in-module proxy for "force-JNI is preserved on the scan path".</p>
 */
public class PluginDrivenScanNodeSysHandleTest {

    @Test
    public void createThreadsSysHandleNotBaseHandleForSysTable() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        // Two DISTINCT handles: a base-table handle and the connector's system-table handle.
        // The base handle stands in for the NORMAL PaimonTableHandle (forceJni=false) that raw
        // getTableHandle would yield; the sys handle stands in for the force-JNI sys handle.
        ConnectorTableHandle baseHandle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableHandle sysHandle = Mockito.mock(ConnectorTableHandle.class);

        TestablePluginCatalog catalog = new TestablePluginCatalog("paimon", metadata, session);
        ExternalDatabase<PluginDrivenExternalTable> db = mockDb("REMOTE_DB");

        // Base handle resolved from the SOURCE remote name (not the "$"-suffixed sys remote name);
        // the connector then maps base handle + "binlog" -> the sys handle. NOTE: there is no stub
        // for getTableHandle on the "$"-suffixed sys remote name, so a raw resolution in create()
        // would return the unstubbed (default) value, never sysHandle.
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.of(baseHandle));
        Mockito.when(metadata.getSysTableHandle(session, baseHandle, "binlog"))
                .thenReturn(Optional.of(sysHandle));

        PluginDrivenExternalTable base = bareTable(catalog, db, "REMOTE_TBL");
        PluginDrivenSysExternalTable sysTable = new PluginDrivenSysExternalTable(base, "binlog") {
            @Override
            protected synchronized void makeSureInitialized() {
                // no-op: skip Env-backed catalog/db init
            }
        };

        PluginDrivenScanNode node = PluginDrivenScanNode.create(new PlanNodeId(0),
                new TupleDescriptor(new TupleId(0)), false, new SessionVariable(),
                ScanContext.EMPTY, catalog, sysTable);

        ConnectorTableHandle resolved = Deencapsulation.getField(node, "currentHandle");
        // WHY: a system-table scan must thread the connector's SYS handle (force-JNI) so binlog/
        // audit_log go through JNI, not the native reader. The sys table's resolveConnectorTableHandle
        // override returns the sys handle; create() MUST honor that seam.
        // MUTATION: reverting create() to raw metadata.getTableHandle(session, dbName,
        // table.getRemoteName()) resolves "REMOTE_TBL$binlog" (unstubbed -> not sysHandle, and
        // never calls getSysTableHandle) -> resolved != sysHandle -> red.
        Assertions.assertSame(sysHandle, resolved,
                "scan node must resolve the connector SYS handle (force-JNI) via the sys-aware seam, "
                        + "not a raw base-table handle");
        Assertions.assertNotSame(baseHandle, resolved,
                "scan node must NOT use the base-table handle for a system table");
        Mockito.verify(metadata).getSysTableHandle(session, baseHandle, "binlog");
    }

    @Test
    public void createUsesBaseHandleForNormalTableUnchanged() {
        // Normal (non-sys) plugin table: base resolveConnectorTableHandle == old inline call, so the
        // resolved handle is exactly metadata.getTableHandle(session, db, remoteName). Pins that the
        // fix is behavior-preserving for normal tables (max_compute/jdbc/etc.).
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle baseHandle = Mockito.mock(ConnectorTableHandle.class);

        TestablePluginCatalog catalog = new TestablePluginCatalog("paimon", metadata, session);
        ExternalDatabase<PluginDrivenExternalTable> db = mockDb("REMOTE_DB");
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.of(baseHandle));

        PluginDrivenExternalTable table = bareTable(catalog, db, "REMOTE_TBL");

        PluginDrivenScanNode node = PluginDrivenScanNode.create(new PlanNodeId(0),
                new TupleDescriptor(new TupleId(0)), false, new SessionVariable(),
                ScanContext.EMPTY, catalog, table);

        ConnectorTableHandle resolved = Deencapsulation.getField(node, "currentHandle");
        // WHY: for a normal table the sys-aware seam's base impl is identical to the old inline
        // getTableHandle, so the resolved handle must still be the plain base handle and the sys
        // path must never be consulted. MUTATION: a create() that always wrapped/forced a sys
        // handle would break normal tables -> this would no longer be baseHandle.
        Assertions.assertSame(baseHandle, resolved,
                "normal plugin table must resolve the plain base-table handle (behavior unchanged)");
        Mockito.verify(metadata, Mockito.never())
                .getSysTableHandle(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    // ==================== helpers (mirror PluginDrivenSysTableTest) ====================

    private static PluginDrivenExternalTable bareTable(PluginDrivenExternalCatalog catalog,
            ExternalDatabase<PluginDrivenExternalTable> db, String remoteName) {
        return new PluginDrivenExternalTable(1L, "tbl", remoteName, catalog, db) {
            @Override
            protected synchronized void makeSureInitialized() {
                // no-op: skip Env-backed catalog/db init
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static ExternalDatabase<PluginDrivenExternalTable> mockDb(String remoteName) {
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getRemoteName()).thenReturn(remoteName);
        return db;
    }

    /**
     * Minimal {@link PluginDrivenExternalCatalog} returning a fixed connector/session without standing
     * up the Doris environment (mirrors {@code PluginDrivenSysTableTest.TestablePluginCatalog}).
     */
    private static class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        private final Connector connector;
        private final ConnectorSession session;

        TestablePluginCatalog(String catalogType, ConnectorMetadata metadata, ConnectorSession session) {
            this(catalogType, mockConnector(metadata, session), session);
        }

        private TestablePluginCatalog(String catalogType, Connector connector, ConnectorSession session) {
            super(1L, "test-catalog", null, makeProps(catalogType), "", connector);
            this.connector = connector;
            this.session = session;
        }

        private static Connector mockConnector(ConnectorMetadata metadata, ConnectorSession session) {
            Connector c = Mockito.mock(Connector.class);
            Mockito.when(c.getMetadata(session)).thenReturn(metadata);
            return c;
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

        private static Map<String, String> makeProps(String type) {
            Map<String, String> props = new HashMap<>();
            props.put("type", type);
            return props;
        }
    }
}
