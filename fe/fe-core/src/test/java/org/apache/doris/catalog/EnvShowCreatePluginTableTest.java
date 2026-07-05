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

package org.apache.doris.catalog;

import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.PluginDrivenSysExternalTable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pins the SHOW CREATE TABLE plugin-driven render arm of {@link Env#getDdlStmt} — the integration point that
 * consumes the connector-pre-rendered SHOW CREATE hints. Covers the three pieces of arm logic not exercised by
 * the {@code PluginDrivenExternalTable} accessor unit tests: (1) the SUPPORTS_SHOW_CREATE_DDL capability gate,
 * (2) the legacy-iceberg clause order (ORDER BY -> PARTITION BY -> LOCATION -> PROPERTIES), and (3) the
 * system-table PARTITION BY suppression. The accessors themselves are stubbed (unit-tested separately); this
 * test is the only automated guard on the Env wiring. (Full byte-level render parity is flip-gated e2e.)
 */
public class EnvShowCreatePluginTableTest {

    private static final List<Column> COLUMNS = ImmutableList.of(
            new Column("id", ScalarType.INT, true, null, true, null, ""),
            new Column("name", ScalarType.createStringType(), false, null, true, null, ""));

    /** Stubs the lead-in (columns/engine/comment) metadata calls getDdlStmt makes before the plugin arm. */
    private static void stubLeadIn(PluginDrivenExternalTable table) {
        Mockito.doReturn(TableType.PLUGIN_EXTERNAL_TABLE).when(table).getType();
        Mockito.doReturn(false).when(table).isTemporary();
        Mockito.doReturn(false).when(table).isManagedTable();
        Mockito.doReturn("t").when(table).getName();
        Mockito.doReturn("").when(table).getComment();
        Mockito.doReturn(COLUMNS).when(table).getBaseSchema(false);
        Mockito.doReturn(TableType.ICEBERG_EXTERNAL_TABLE.name()).when(table).getEngineTableTypeName();
    }

    private static String renderDdl(PluginDrivenExternalTable table) {
        List<String> createTableStmt = new ArrayList<>();
        Env.getDdlStmt(null, "mydb", table, createTableStmt, new ArrayList<>(), new ArrayList<>(),
                false, false, false, -1L, false, false);
        Assertions.assertEquals(1, createTableStmt.size());
        return createTableStmt.get(0);
    }

    @Test
    public void rendersClausesInLegacyOrderWhenConnectorSupportsShowCreate() {
        PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        stubLeadIn(table);
        Mockito.doReturn(true).when(table).supportsShowCreateDdl();
        Mockito.doReturn("ORDER BY (`name` DESC NULLS LAST)").when(table).getShowSortClause();
        Mockito.doReturn("PARTITION BY LIST (BUCKET(8, `id`)) ()").when(table).getShowPartitionClause();
        Mockito.doReturn("s3://bucket/db/t").when(table).getShowLocation();
        Map<String, String> props = new LinkedHashMap<>();
        props.put("write.format.default", "parquet");
        Mockito.doReturn(props).when(table).getTableProperties();

        String ddl = renderDdl(table);

        Assertions.assertTrue(ddl.contains("ORDER BY (`name` DESC NULLS LAST)"), ddl);
        Assertions.assertTrue(ddl.contains("PARTITION BY LIST (BUCKET(8, `id`)) ()"), ddl);
        Assertions.assertTrue(ddl.contains("LOCATION 's3://bucket/db/t'"), ddl);
        Assertions.assertTrue(ddl.contains("\"write.format.default\" = \"parquet\""), ddl);
        // WHY: the clause order must mirror the legacy iceberg arm exactly (ORDER BY before PARTITION BY before
        // LOCATION before PROPERTIES). MUTATION: reordering any append, or reading the wrong getShow* accessor
        // for a clause -> the index ordering breaks -> red.
        int sortIdx = ddl.indexOf("ORDER BY (");
        int partIdx = ddl.indexOf("PARTITION BY LIST");
        int locIdx = ddl.indexOf("LOCATION '");
        int propIdx = ddl.indexOf("PROPERTIES (");
        Assertions.assertTrue(sortIdx >= 0 && sortIdx < partIdx, ddl);
        Assertions.assertTrue(partIdx < locIdx, ddl);
        Assertions.assertTrue(locIdx < propIdx, ddl);
    }

    @Test
    public void rendersCommentOnlyWhenConnectorDoesNotSupportShowCreate() {
        // A connector that does NOT declare SUPPORTS_SHOW_CREATE_DDL (jdbc/es: credential-bearing properties)
        // must stay comment-only — no LOCATION/PROPERTIES/PARTITION/ORDER. MUTATION: dropping the capability
        // gate (always render) -> these clauses appear (leaking jdbc connection props) -> red.
        PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        stubLeadIn(table);
        Mockito.doReturn(false).when(table).supportsShowCreateDdl();
        // Even if accessors would return content, the gate must suppress everything. (lenient: not invoked)
        Mockito.lenient().doReturn("ORDER BY (`name` ASC NULLS FIRST)").when(table).getShowSortClause();
        Mockito.lenient().doReturn("s3://leak").when(table).getShowLocation();

        String ddl = renderDdl(table);

        Assertions.assertFalse(ddl.contains("LOCATION '"), ddl);
        Assertions.assertFalse(ddl.contains("PROPERTIES ("), ddl);
        Assertions.assertFalse(ddl.contains("PARTITION BY"), ddl);
        Assertions.assertFalse(ddl.contains("ORDER BY"), ddl);
        // The engine line is still rendered (this is a real table DDL, just without the connector specifics).
        Assertions.assertTrue(ddl.contains("ENGINE=" + TableType.ICEBERG_EXTERNAL_TABLE.name()), ddl);
    }

    @Test
    public void suppressesPartitionClauseForSystemTable() {
        // A system table ($snapshots etc.) renders its SOURCE table's DDL but NOT a PARTITION BY clause
        // (mirroring the legacy arm, which gated partitions on the table being the data table). The sort clause
        // still renders from the source. MUTATION: dropping the isSysTable guard -> PARTITION BY appears -> red.
        PluginDrivenExternalTable source =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(true).when(source).supportsShowCreateDdl();
        Mockito.doReturn("ORDER BY (`name` DESC NULLS LAST)").when(source).getShowSortClause();
        Mockito.doReturn("PARTITION BY LIST (`id`) ()").when(source).getShowPartitionClause();
        Mockito.doReturn("s3://bucket/db/t").when(source).getShowLocation();
        Mockito.doReturn(new LinkedHashMap<String, String>()).when(source).getTableProperties();

        PluginDrivenSysExternalTable sysTable =
                Mockito.mock(PluginDrivenSysExternalTable.class, Mockito.CALLS_REAL_METHODS);
        stubLeadIn(sysTable);
        Mockito.doReturn(source).when(sysTable).getSourceTable();

        String ddl = renderDdl(sysTable);

        Assertions.assertTrue(ddl.contains("ORDER BY (`name` DESC NULLS LAST)"), ddl);
        Assertions.assertTrue(ddl.contains("LOCATION 's3://bucket/db/t'"), ddl);
        Assertions.assertFalse(ddl.contains("PARTITION BY"),
                "a system table must not render a PARTITION BY clause: " + ddl);
    }
}
