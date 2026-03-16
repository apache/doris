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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.Set;

public class IcebergConflictDetectionFilterUtilsTest {

    private IcebergExternalTable targetTable;

    @Before
    public void setUp() {
        Table icebergTable = Mockito.mock(Table.class);
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
        Mockito.when(icebergTable.schema()).thenReturn(schema);

        targetTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(targetTable.getId()).thenReturn(1L);
        Mockito.when(targetTable.getIcebergTable()).thenReturn(icebergTable);
    }

    @Test
    public void testBuildConflictDetectionFilterWithTargetPredicate() {
        SlotReference slot = buildSlot(targetTable, "id", ScalarType.INT);
        Expression predicate = new EqualTo(slot, new IntegerLiteral(1));
        Plan plan = buildPlan(ImmutableSet.of(predicate), slot);

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(plan, targetTable);

        Assert.assertTrue(result.isPresent());
    }

    @Test
    public void testBuildConflictDetectionFilterIgnoreNonTargetPredicate() {
        IcebergExternalTable otherTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(otherTable.getId()).thenReturn(2L);
        SlotReference slot = buildSlot(otherTable, "id", ScalarType.INT);
        Expression predicate = new EqualTo(slot, new IntegerLiteral(1));
        Plan plan = buildPlan(ImmutableSet.of(predicate), slot);

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(plan, targetTable);

        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testBuildConflictDetectionFilterKeepTargetPredicateInMixedConjuncts() {
        IcebergExternalTable otherTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(otherTable.getId()).thenReturn(2L);

        SlotReference targetSlot = buildSlot(targetTable, "id", ScalarType.INT);
        SlotReference otherSlot = buildSlot(otherTable, "id", ScalarType.INT);
        Set<Expression> conjuncts = ImmutableSet.of(
                new EqualTo(targetSlot, new IntegerLiteral(1)),
                new EqualTo(otherSlot, new IntegerLiteral(2)));
        Plan plan = buildPlan(conjuncts, targetSlot);

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(plan, targetTable);

        Assert.assertTrue(result.isPresent());
    }

    @Test
    public void testBuildConflictDetectionFilterIgnoreRowIdPredicate() {
        SlotReference slot = buildSlot(targetTable, Column.ICEBERG_ROWID_COL, ScalarType.STRING);
        Expression predicate = new EqualTo(slot, new StringLiteral("rowid"));
        Plan plan = buildPlan(ImmutableSet.of(predicate), slot);

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(plan, targetTable);

        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testBuildConflictDetectionFilterIgnoreMetadataPredicate() {
        SlotReference slot = buildSlot(targetTable, IcebergMetadataColumn.FILE_PATH.getColumnName(),
                ScalarType.STRING);
        Expression predicate = new EqualTo(slot, new StringLiteral("/path/a.parquet"));
        Plan plan = buildPlan(ImmutableSet.of(predicate), slot);

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(plan, targetTable);

        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testBuildConflictDetectionFilterAllowsOrOnSameColumn() {
        SlotReference slot = buildSlot(targetTable, "id", ScalarType.INT);
        Expression predicate = new Or(new EqualTo(slot, new IntegerLiteral(1)),
                new EqualTo(slot, new IntegerLiteral(2)));
        Plan plan = buildPlan(ImmutableSet.of(predicate), slot);

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(plan, targetTable);

        Assert.assertTrue(result.isPresent());
    }

    @Test
    public void testBuildConflictDetectionFilterIgnoreOrOnDifferentColumns() {
        SlotReference idSlot = buildSlot(targetTable, "id", ScalarType.INT);
        SlotReference nameSlot = buildSlot(targetTable, "name", ScalarType.STRING);
        Expression predicate = new Or(new EqualTo(idSlot, new IntegerLiteral(1)),
                new EqualTo(nameSlot, new StringLiteral("a")));
        Plan plan = buildPlan(ImmutableSet.of(predicate), idSlot);

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(plan, targetTable);

        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testBuildConflictDetectionFilterAllowsIsNotNull() {
        SlotReference slot = buildSlot(targetTable, "id", ScalarType.INT);
        Expression predicate = new Not(new IsNull(slot), true);
        Plan plan = buildPlan(ImmutableSet.of(predicate), slot);

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(plan, targetTable);

        Assert.assertTrue(result.isPresent());
    }

    @Test
    public void testBuildConflictDetectionFilterIgnoreNotPredicate() {
        SlotReference slot = buildSlot(targetTable, "id", ScalarType.INT);
        Expression predicate = new Not(new EqualTo(slot, new IntegerLiteral(1)));
        Plan plan = buildPlan(ImmutableSet.of(predicate), slot);

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(plan, targetTable);

        Assert.assertFalse(result.isPresent());
    }

    private SlotReference buildSlot(TableIf table, String name, ScalarType type) {
        Column column = new Column(name, type);
        return SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), table, column, ImmutableList.of());
    }

    private Plan buildPlan(Set<Expression> conjuncts, SlotReference outputSlot) {
        LogicalEmptyRelation child = new LogicalEmptyRelation(new RelationId(0),
                ImmutableList.of((NamedExpression) outputSlot));
        return new LogicalFilter<>(conjuncts, child);
    }
}
