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
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
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

    @Test
    public void handlesIcebergTableOnly() {
        Assertions.assertTrue(transform.handles(Mockito.mock(IcebergExternalTable.class)));
        Assertions.assertFalse(transform.handles(Mockito.mock(TableIf.class)));
        Assertions.assertFalse(transform.handles(null));
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
