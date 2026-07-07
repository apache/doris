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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests {@link PluginDrivenExternalTable#fetchRowCount}'s two-layer statistics consumption (§4.2 read-side
 * SPI): (1) an exact connector row count is used directly; (2) when the connector reports an on-disk data
 * size but no exact count, fe-core estimates the cardinality as {@code dataSize / <Doris row width>} — the
 * type-dependent division the connector cannot do. The row width is summed over the FULL schema (partition
 * columns included), mirroring legacy {@code StatisticsUtil.getHiveRowCount}. This branch is
 * connector-agnostic: every non-hive connector reports dataSize -1, leaving it inert.
 */
public class PluginDrivenExternalTableRowCountTest {

    private static Column intCol(String name) {
        return new Column(name, PrimitiveType.INT);      // slot size 4
    }

    private static Column bigintCol(String name) {
        return new Column(name, PrimitiveType.BIGINT);   // slot size 8
    }

    @Test
    public void exactRowCountUsedDirectlyIgnoringDataSize() {
        // A present rowCount >= 0 short-circuits: dataSize is not consulted even when set. MUTATION:
        // preferring the dataSize estimate would return 5000/4=1250, not 1234 -> red.
        PluginDrivenExternalTable table = tableReturning(
                Optional.of(new ConnectorTableStatistics(1234, 5000)),
                Collections.singletonList(intCol("v")));
        Assertions.assertEquals(1234L, table.fetchRowCount());
    }

    @Test
    public void dataSizeEstimatedOverFullSchemaWidth() {
        // rowCount UNKNOWN(-1) + dataSize 4000, schema 10x INT (width 40) -> 4000/40 = 100. MUTATION:
        // not estimating (returning UNKNOWN) -> red; wrong width -> wrong number.
        List<Column> schema = Arrays.asList(
                intCol("c0"), intCol("c1"), intCol("c2"), intCol("c3"), intCol("c4"),
                intCol("c5"), intCol("c6"), intCol("c7"), intCol("c8"), intCol("c9"));
        PluginDrivenExternalTable table = tableReturning(
                Optional.of(new ConnectorTableStatistics(-1, 4000)), schema);
        Assertions.assertEquals(100L, table.fetchRowCount());
    }

    @Test
    public void partitionColumnsCountTowardRowWidth() {
        // WHY: legacy getHiveRowCount summed the row width over the FULL schema, INCLUDING partition
        // columns. Schema = INT(4) data + BIGINT(8) partition -> width 12; dataSize 1200 -> 100. A
        // mutation excluding partition columns would use width 4 -> 300 -> red.
        List<Column> schema = Arrays.asList(intCol("v"), bigintCol("dt"));
        PluginDrivenExternalTable table = tableReturning(
                Optional.of(new ConnectorTableStatistics(-1, 1200)), schema);
        Assertions.assertEquals(100L, table.fetchRowCount());
    }

    @Test
    public void emptyStatisticsYieldUnknown() {
        PluginDrivenExternalTable table = tableReturning(
                Optional.empty(), Collections.singletonList(intCol("v")));
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, table.fetchRowCount());
    }

    @Test
    public void dataSizeWithEmptySchemaYieldsUnknownNotDivideByZero() {
        // width 0 -> "cannot estimate" -> UNKNOWN (not an ArithmeticException). MUTATION: dividing by a
        // 0 width throws -> red.
        PluginDrivenExternalTable table = tableReturning(
                Optional.of(new ConnectorTableStatistics(-1, 4000)), Collections.emptyList());
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, table.fetchRowCount());
    }

    @Test
    public void unresolvableHandleYieldsUnknown() {
        PluginDrivenExternalTable table = tableReturning(
                null, Collections.singletonList(intCol("v")));   // null -> getTableHandle returns empty
        Assertions.assertEquals(TableIf.UNKNOWN_ROW_COUNT, table.fetchRowCount());
    }

    /**
     * Builds a table over a mock connector. {@code stats} == null makes {@code getTableHandle} return empty
     * (unresolvable handle); otherwise the handle resolves and {@code getTableStatistics} returns {@code stats}.
     * The full schema is served from a stubbed schema-cache value.
     */
    private static PluginDrivenExternalTable tableReturning(
            Optional<ConnectorTableStatistics> stats, List<Column> schema) {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        if (stats == null) {
            Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                    .thenReturn(Optional.empty());
        } else {
            ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
            Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                    .thenReturn(Optional.of(handle));
            Mockito.when(metadata.getTableStatistics(session, handle)).thenReturn(stats);
        }
        TestablePluginCatalog catalog = new TestablePluginCatalog(metadata, session);

        @SuppressWarnings("unchecked")
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getRemoteName()).thenReturn("REMOTE_DB");

        PluginDrivenSchemaCacheValue cacheValue = new PluginDrivenSchemaCacheValue(
                schema, Collections.emptyList(), Collections.emptyList());
        return new PluginDrivenExternalTable(1L, "tbl", "REMOTE_TBL", catalog, db) {
            @Override
            protected synchronized void makeSureInitialized() {
                // no-op: skip Env-backed catalog/db init
            }

            @Override
            public Optional<SchemaCacheValue> getSchemaCacheValue() {
                return Optional.of(cacheValue);
            }
        };
    }

    /** Minimal catalog returning a fixed connector/session without standing up the Doris environment. */
    private static class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        private final Connector connector;
        private final ConnectorSession session;

        TestablePluginCatalog(ConnectorMetadata metadata, ConnectorSession session) {
            this(mockConnector(metadata, session), session);
        }

        private TestablePluginCatalog(Connector connector, ConnectorSession session) {
            super(1L, "test-catalog", null, props(), "", connector);
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

        private static Map<String, String> props() {
            Map<String, String> props = new HashMap<>();
            props.put("type", "hms");
            return props;
        }
    }
}
