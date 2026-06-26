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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.Set;

/**
 * Unit tests for {@link IcebergRowLevelDmlTransform} (P6.3-T07c).
 *
 * <p>Covers the genuinely new T07c logic: the registry table-type predicate, the frozen per-op label
 * prefixes (profile/txn parity), the O5-2 synthetic-column exclusion supplied to {@link WriteConstraintExtractor
 * via the iceberg-specific predicate}, and the dormant O5-2 {@code applyWriteConstraint} round-trip. The
 * synthesis/executor/sink delegation is exercised end-to-end by {@code IcebergDDLAndDMLPlanTest}.</p>
 */
public class IcebergRowLevelDmlTransformTest {

    private static final long TARGET_ID = 7L;
    private final IcebergRowLevelDmlTransform transform = new IcebergRowLevelDmlTransform();

    private SlotReference slot(TableIf table, String name) {
        return SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), table,
                new Column(name, ScalarType.INT), ImmutableList.of());
    }

    private Plan filterOver(TableIf table, String columnName) {
        SlotReference slot = slot(table, columnName);
        Set<Expression> conjuncts = ImmutableSet.of(new EqualTo(slot, new IntegerLiteral(1)));
        LogicalEmptyRelation child = new LogicalEmptyRelation(new RelationId(0),
                ImmutableList.of((NamedExpression) slot));
        return new LogicalFilter<>(conjuncts, child);
    }

    /**
     * A {@link PluginDrivenExternalTable} whose connector reports the given row-level DML capabilities.
     * Mirrors {@code InsertOverwriteTableCommandTest.pluginTable} — the established way to exercise the
     * {@code getConnector().getMetadata(buildConnectorSession()).supportsX()} probe.
     */
    private static PluginDrivenExternalTable pluginTable(boolean supportsDelete, boolean supportsMerge) {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
        Mockito.when(metadata.supportsDelete()).thenReturn(supportsDelete);
        Mockito.when(metadata.supportsMerge()).thenReturn(supportsMerge);
        return table;
    }

    @Test
    public void handlesLegacyIcebergExternalTable() {
        // Pre-flip parity: the legacy concrete iceberg table type is admitted unconditionally.
        Assertions.assertTrue(transform.handles(Mockito.mock(IcebergExternalTable.class)));
        Assertions.assertFalse(transform.handles(Mockito.mock(TableIf.class)));
        Assertions.assertFalse(transform.handles(null));
    }

    @Test
    public void handlesPluginDrivenTableByRowLevelDmlCapability() {
        // Post-flip: an iceberg table presents as PluginDrivenExternalTable; it is admitted via the
        // neutral connector capability (supportsDelete || supportsMerge), NOT a concrete iceberg cast.
        Assertions.assertTrue(transform.handles(pluginTable(true, false)));
        Assertions.assertTrue(transform.handles(pluginTable(false, true)));
        Assertions.assertTrue(transform.handles(pluginTable(true, true)));
        // A plugin connector with neither capability (e.g. jdbc/es/paimon today) must NOT be admitted,
        // else its row-level DML would route through the iceberg synthesis path.
        Assertions.assertFalse(transform.handles(pluginTable(false, false)));
    }

    /**
     * A {@link PluginDrivenExternalTable} (db1.t1) whose connector resolves to {@code metadata}. Used to
     * drive the post-flip {@link IcebergRowLevelDmlTransform#checkMode} plugin arm, which routes the
     * copy-on-write rejection through the connector's neutral {@code validateRowLevelDmlMode} SPI.
     */
    private static PluginDrivenExternalTable pluginTableWithMetadata(
            ConnectorMetadata metadata, ConnectorSession session) {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getRemoteDbName()).thenReturn("db1");
        Mockito.when(table.getRemoteName()).thenReturn("t1");
        Mockito.when(catalog.getName()).thenReturn("ice_cat");
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
        return table;
    }

    @Test
    public void checkModePluginArmRoutesEachOpToConnectorMode() {
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "db1", "t1")).thenReturn(Optional.of(handle));
        PluginDrivenExternalTable table = pluginTableWithMetadata(metadata, session);

        transform.checkMode(table, RowLevelDmlOp.DELETE);
        transform.checkMode(table, RowLevelDmlOp.UPDATE);
        transform.checkMode(table, RowLevelDmlOp.MERGE);

        // Post-flip: the mode check delegates to the connector SPI on the resolved handle. The neutral op axis
        // must map DELETE->DELETE, UPDATE->UPDATE, MERGE->MERGE so the connector reads the matching
        // write.{delete,update,merge}.mode property. MUTATION: collapsing toWriteOperation to one value -> the
        // wrong property is checked -> these verifies fail.
        Mockito.verify(metadata).validateRowLevelDmlMode(session, handle, WriteOperation.DELETE);
        Mockito.verify(metadata).validateRowLevelDmlMode(session, handle, WriteOperation.UPDATE);
        Mockito.verify(metadata).validateRowLevelDmlMode(session, handle, WriteOperation.MERGE);
    }

    @Test
    public void checkModePluginArmWrapsConnectorRejectionAsAnalysisException() {
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "db1", "t1")).thenReturn(Optional.of(handle));
        Mockito.doThrow(new DorisConnectorException(
                        "Doris does not support DELETE on Iceberg copy-on-write tables."))
                .when(metadata).validateRowLevelDmlMode(session, handle, WriteOperation.DELETE);
        PluginDrivenExternalTable table = pluginTableWithMetadata(metadata, session);

        // The connector throws its own DorisConnectorException; the transform surfaces it as the analysis-time
        // AnalysisException the legacy path threw, preserving the message. MUTATION: dropping the catch/rethrow
        // -> a DorisConnectorException escapes (wrong type) -> red.
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> transform.checkMode(table, RowLevelDmlOp.DELETE));
        Assertions.assertTrue(e.getMessage().contains("copy-on-write"), e.getMessage());
    }

    @Test
    public void checkModePluginArmThrowsWhenTableHandleMissing() {
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.getTableHandle(session, "db1", "t1")).thenReturn(Optional.empty());
        PluginDrivenExternalTable table = pluginTableWithMetadata(metadata, session);

        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> transform.checkMode(table, RowLevelDmlOp.DELETE));
        Assertions.assertTrue(e.getMessage().contains("Table not found"), e.getMessage());
    }

    @Test
    public void labelPrefixIsFrozenPerOp() {
        // These are profile/txn-visible and must stay byte-identical to the legacy Iceberg*Command labels.
        Assertions.assertEquals("iceberg_delete", transform.labelPrefix(RowLevelDmlOp.DELETE));
        Assertions.assertEquals("iceberg_update_merge", transform.labelPrefix(RowLevelDmlOp.UPDATE));
        Assertions.assertEquals("iceberg_merge_into", transform.labelPrefix(RowLevelDmlOp.MERGE));
    }

    @Test
    public void extractWriteConstraintKeepsRegularTargetColumn() {
        TableIf target = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(target.getId()).thenReturn(TARGET_ID);
        Optional<ConnectorPredicate> result = transform.extractWriteConstraint(filterOver(target, "id"), target);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void extractWriteConstraintExcludesRowIdColumn() {
        // Load-bearing: the synthetic $row_id slot has originalTable == target, so the origin-table check alone
        // would keep it; only the iceberg ICEBERG_EXCLUSION predicate drops it (closes T07b critic BLOCKER).
        TableIf target = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(target.getId()).thenReturn(TARGET_ID);
        Plan plan = filterOver(target, Column.ICEBERG_ROWID_COL);
        Assertions.assertFalse(transform.extractWriteConstraint(plan, target).isPresent());
    }

    @Test
    public void extractWriteConstraintExcludesMetadataColumn() {
        TableIf target = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(target.getId()).thenReturn(TARGET_ID);
        // "$partition_spec_id" is an IcebergMetadataColumn -> excluded.
        Plan plan = filterOver(target, "$partition_spec_id");
        Assertions.assertFalse(transform.extractWriteConstraint(plan, target).isPresent());
    }

    @Test
    public void applyWriteConstraintPushesToConnectorTransactionWhenPresent() {
        // O5-2 round-trip (D2): when the executor exposes an SPI ConnectorTransaction, the extracted predicate
        // is pushed onto it via applyWriteConstraint. (Reachable only post-P6.6 in production; tested via stub.)
        RowLevelDmlTransform t = Mockito.mock(RowLevelDmlTransform.class);
        BaseExternalTableInsertExecutor executor = Mockito.mock(BaseExternalTableInsertExecutor.class);
        ConnectorTransaction connectorTx = Mockito.mock(ConnectorTransaction.class);
        ConnectorPredicate predicate = Mockito.mock(ConnectorPredicate.class);
        TableIf table = Mockito.mock(TableIf.class);
        Plan analyzedPlan = Mockito.mock(Plan.class);

        Mockito.when(executor.getConnectorTransactionOrNull()).thenReturn(connectorTx);
        Mockito.when(t.extractWriteConstraint(analyzedPlan, table)).thenReturn(Optional.of(predicate));

        RowLevelDmlCommand.applyWriteConstraintIfPresent(t, executor, analyzedPlan, table);

        Mockito.verify(connectorTx).applyWriteConstraint(predicate);
    }

    @Test
    public void applyWriteConstraintIsNoOpOnLegacyTransactionPath() {
        // Dormant today: iceberg DELETE/MERGE run on the legacy IcebergTransaction, so the base executor's
        // getConnectorTransactionOrNull() returns null and the O5-2 path is skipped entirely.
        RowLevelDmlTransform t = Mockito.mock(RowLevelDmlTransform.class);
        BaseExternalTableInsertExecutor executor = Mockito.mock(BaseExternalTableInsertExecutor.class);
        TableIf table = Mockito.mock(TableIf.class);
        Plan analyzedPlan = Mockito.mock(Plan.class);

        Mockito.when(executor.getConnectorTransactionOrNull()).thenReturn(null);

        RowLevelDmlCommand.applyWriteConstraintIfPresent(t, executor, analyzedPlan, table);

        Mockito.verify(t, Mockito.never()).extractWriteConstraint(Mockito.any(), Mockito.any());
    }
}
