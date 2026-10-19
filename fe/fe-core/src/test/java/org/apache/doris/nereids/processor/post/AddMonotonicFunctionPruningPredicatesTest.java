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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.functions.table.Local;
import org.apache.doris.nereids.trees.expressions.functions.table.Numbers;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class AddMonotonicFunctionPruningPredicatesTest extends ExpressionRewriteTestHelper {

    private final AddMonotonicFunctionPruningPredicates processor =
            new AddMonotonicFunctionPruningPredicates();

    @Test
    void addYearRangeForOlapScan() {
        SlotReference dateTimeSlot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT, true);
        Expression original = yearEquals(dateTimeSlot);
        PhysicalFilter<PhysicalOlapScan> filter = new PhysicalFilter<>(
                ImmutableSet.of(original), logicalProperties(dateTimeSlot), olapScan(dateTimeSlot));

        Plan rewritten = filter.accept(processor, cascadesContext);

        assertYearRange(rewritten, original, dateTimeSlot);
        Assertions.assertSame(rewritten, rewritten.accept(processor, cascadesContext));
    }

    @Test
    void addYearRangeForFileScan() {
        SlotReference dateTimeSlot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT, true);
        Expression original = yearEquals(dateTimeSlot);
        PhysicalFilter<PhysicalFileScan> filter = new PhysicalFilter<>(
                ImmutableSet.of(original), logicalProperties(dateTimeSlot), fileScan(dateTimeSlot, true));

        Plan rewritten = filter.accept(processor, cascadesContext);

        assertYearRange(rewritten, original, dateTimeSlot);
    }

    @Test
    void doNotAddPredicatesForNonStorageFileScan() {
        SlotReference dateTimeSlot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT, true);
        PhysicalFilter<PhysicalFileScan> filter = new PhysicalFilter<>(
                ImmutableSet.of(yearEquals(dateTimeSlot)), logicalProperties(dateTimeSlot),
                fileScan(dateTimeSlot, false));

        Assertions.assertSame(filter, filter.accept(processor, cascadesContext));
    }

    @Test
    void addYearRangeThroughStorageLayerAggregate() {
        SlotReference dateTimeSlot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT, true);
        Expression original = yearEquals(dateTimeSlot);
        PhysicalStorageLayerAggregate storageAggregate = new PhysicalStorageLayerAggregate(
                olapScan(dateTimeSlot), PushDownAggOp.COUNT_ON_MATCH);
        PhysicalFilter<PhysicalStorageLayerAggregate> filter = new PhysicalFilter<>(
                ImmutableSet.of(original), logicalProperties(dateTimeSlot), storageAggregate);

        Plan rewritten = filter.accept(processor, cascadesContext);

        assertYearRange(rewritten, original, dateTimeSlot);
    }

    @Test
    void addExactPrefixRangeForOlapScan() {
        SlotReference stringSlot = new SlotReference("v", VarcharType.createVarcharType(32), true);
        Expression original = typeCoercion(new EqualTo(
                new Substring(stringSlot, new IntegerLiteral(1), new IntegerLiteral(3)),
                new VarcharLiteral("abc")));
        PhysicalFilter<PhysicalOlapScan> filter = new PhysicalFilter<>(
                ImmutableSet.of(original), logicalProperties(stringSlot), olapScan(stringSlot));

        Plan rewritten = filter.accept(processor, cascadesContext);

        Set<Expression> conjuncts = ((PhysicalFilter<?>) rewritten).getConjuncts();
        Assertions.assertEquals(3, conjuncts.size());
        Assertions.assertTrue(conjuncts.stream().anyMatch(conjunct -> conjunct instanceof GreaterThanEqual
                && conjunct.child(0).equals(stringSlot) && conjunct.isInferred()));
        Assertions.assertTrue(conjuncts.stream().anyMatch(conjunct -> conjunct instanceof LessThan
                && conjunct.child(0).equals(stringSlot) && conjunct.isInferred()));
    }

    @Test
    void addYearRangeForFileTableValuedFunction() {
        SlotReference dateTimeSlot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT, true);
        Expression original = yearEquals(dateTimeSlot);
        PhysicalTVFRelation tvf = new PhysicalTVFRelation(
                RelationId.createGenerator().getNextId(),
                new Local(new Properties(Collections.emptyMap())),
                ImmutableList.of(dateTimeSlot), logicalProperties(dateTimeSlot));
        PhysicalFilter<PhysicalTVFRelation> filter = new PhysicalFilter<>(
                ImmutableSet.of(original), logicalProperties(dateTimeSlot), tvf);

        Plan rewritten = filter.accept(processor, cascadesContext);

        assertYearRange(rewritten, original, dateTimeSlot);
    }

    @Test
    void doNotAddPredicatesAboveNonFileTableValuedFunction() {
        SlotReference dateTimeSlot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT, true);
        PhysicalTVFRelation tvf = new PhysicalTVFRelation(
                RelationId.createGenerator().getNextId(),
                new Numbers(new Properties(Collections.emptyMap())),
                ImmutableList.of(dateTimeSlot), logicalProperties(dateTimeSlot));
        PhysicalFilter<PhysicalTVFRelation> filter = new PhysicalFilter<>(
                ImmutableSet.of(yearEquals(dateTimeSlot)), logicalProperties(dateTimeSlot), tvf);

        Assertions.assertSame(filter, filter.accept(processor, cascadesContext));
    }

    @Test
    void doNotAddPredicatesAboveNonScanPlan() {
        SlotReference dateTimeSlot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT, true);
        LogicalProperties logicalProperties = logicalProperties(dateTimeSlot);
        PhysicalEmptyRelation emptyRelation = new PhysicalEmptyRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(dateTimeSlot), logicalProperties);
        PhysicalFilter<PhysicalEmptyRelation> filter = new PhysicalFilter<>(
                ImmutableSet.of(yearEquals(dateTimeSlot)), logicalProperties, emptyRelation);

        Assertions.assertSame(filter, filter.accept(processor, cascadesContext));
    }

    private Expression yearEquals(SlotReference dateTimeSlot) {
        return typeCoercion(new EqualTo(new Year(dateTimeSlot), new IntegerLiteral(2026)));
    }

    private void assertYearRange(Plan plan, Expression original, SlotReference dateTimeSlot) {
        Assertions.assertInstanceOf(PhysicalFilter.class, plan);
        Set<Expression> conjuncts = ((PhysicalFilter<?>) plan).getConjuncts();
        Assertions.assertEquals(3, conjuncts.size());
        Assertions.assertTrue(conjuncts.stream()
                .anyMatch(conjunct -> conjunct.equals(original) && !conjunct.isInferred()));
        Assertions.assertTrue(conjuncts.stream()
                .anyMatch(conjunct -> conjunct instanceof GreaterThanEqual
                        && conjunct.child(0).equals(dateTimeSlot) && conjunct.isInferred()));
        Assertions.assertTrue(conjuncts.stream()
                .anyMatch(conjunct -> conjunct instanceof LessThan
                        && conjunct.child(0).equals(dateTimeSlot) && conjunct.isInferred()));
    }

    private PhysicalOlapScan olapScan(SlotReference output) {
        OlapTable table = PlanConstructor.newOlapTable(0, "t", 0, KeysType.DUP_KEYS);
        return new PhysicalOlapScan(RelationId.createGenerator().getNextId(), table,
                ImmutableList.of("test"), 0L, Collections.emptyList(), Collections.emptyList(), null,
                PreAggStatus.on(), ImmutableList.of(), Optional.empty(), logicalProperties(output),
                Optional.empty(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty(),
                Optional.empty(), ImmutableList.of(), Optional.empty());
    }

    private PhysicalFileScan fileScan(SlotReference output, boolean supportsStoragePredicatePruning) {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.supportsStoragePredicatePruning()).thenReturn(supportsStoragePredicatePruning);
        return new PhysicalFileScan(RelationId.createGenerator().getNextId(), table,
                ImmutableList.of("test"), null, Optional.empty(), logicalProperties(output),
                SelectedPartitions.NOT_PRUNED, Optional.empty(), Optional.empty(), ImmutableList.of(),
                Optional.empty());
    }

    private LogicalProperties logicalProperties(Slot output) {
        List<Slot> outputs = ImmutableList.of(output);
        return new LogicalProperties(() -> outputs, () -> DataTrait.EMPTY_TRAIT);
    }
}
