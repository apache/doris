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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.Set;

/**
 * Unit tests for the row-id injection half of {@link RowLevelDmlRowIdUtils} (the SDK expression-conversion
 * half was removed together with its dead legacy callers).
 */
public class RowLevelDmlRowIdUtilsTest {

    // ==================== isRowIdInjectionTarget (row-id injection guard) ====================

    /** A plugin-driven table whose connector declares the given row-level-DML capabilities. */
    private static PluginDrivenExternalTable pluginTableWithCapability(boolean supportsDelete, boolean supportsMerge) {
        Set<WriteOperation> ops = EnumSet.noneOf(WriteOperation.class);
        if (supportsDelete) {
            ops.add(WriteOperation.DELETE);
        }
        if (supportsMerge) {
            ops.add(WriteOperation.MERGE);
        }
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.supportedWriteOperations()).thenReturn(ops);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        return table;
    }

    @Test
    public void isRowIdInjectionTargetAcceptsDeleteOnlyPluginDrivenTable() {
        // An iceberg PluginDrivenExternalTable is recognized by the neutral capability
        // (supportsDelete OR supportsMerge). delete-only (true,false) pins the delete arm + an OR->AND mutation
        // (which would reject it). MUTATION: dropping the plugin arm makes this red (row-id injection
        // would never fire).
        Assertions.assertTrue(
                RowLevelDmlRowIdUtils.isRowIdInjectionTarget(pluginTableWithCapability(true, false)));
    }

    @Test
    public void isRowIdInjectionTargetAcceptsMergeOnlyPluginDrivenTable() {
        // merge-only (false,true) pins the OTHER arm of the OR: iceberg supports MERGE, so a 'drop
        // ||supportsMerge()' mutation (return supportsDelete()) must die here. Without this case the delete-only
        // test above leaves that mutation surviving.
        Assertions.assertTrue(
                RowLevelDmlRowIdUtils.isRowIdInjectionTarget(pluginTableWithCapability(false, true)));
    }

    @Test
    public void isRowIdInjectionTargetRejectsPluginDrivenTableWithoutCapability() {
        // A non-iceberg plugin-driven table (jdbc/es/trino/max_compute/paimon) declares neither capability,
        // so it is not a row-id-injection target — the guard must not inject into its scans.
        Assertions.assertFalse(
                RowLevelDmlRowIdUtils.isRowIdInjectionTarget(pluginTableWithCapability(false, false)));
    }

    @Test
    public void isRowIdInjectionTargetRejectsUnrelatedExternalTable() {
        // Any other table type (e.g. an HMS/olap external table) is never a row-id-injection target.
        Assertions.assertFalse(
                RowLevelDmlRowIdUtils.isRowIdInjectionTarget(Mockito.mock(ExternalTable.class)));
    }

    @Test
    public void isRowIdInjectionTargetDegradesWhenConnectorDropped() {
        // A catalog dropped mid-DML-planning nulls its transient connector; the guard must degrade to
        // "not a target" rather than NPE-aborting the query, mirroring the defensive
        // PluginDrivenExternalTable.fetchSyntheticWriteColumns. MUTATION: dropping the null-connector guard
        // NPEs here.
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(null);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);

        Assertions.assertFalse(RowLevelDmlRowIdUtils.isRowIdInjectionTarget(table));
    }
}
