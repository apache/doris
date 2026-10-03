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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.implementation.AggregateStrategies;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ln;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class PhysicalStorageLayerAggregateTest implements MemoPatternMatchSupported {

    @Test
    public void testWithoutProject() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(1, "tbl", 0);
        LogicalAggregate<LogicalOlapScan> aggregate;
        CascadesContext context;

        // min max
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Min(olapScan.getOutput().get(0)), "min")),
                true, Optional.empty(), olapScan);
        context = MemoTestUtils.createCascadesContext(aggregate);

        PlanChecker.from(context)
                .applyImplementation(storageLayerAggregateWithoutProject())
                .matches(
                    logicalAggregate(
                        physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.MIN_MAX)
                    )
                );
        // count
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(olapScan.getOutput().get(0)), "count")),
                true, Optional.empty(), olapScan);
        context = MemoTestUtils.createCascadesContext(aggregate);

        PlanChecker.from(context)
                .applyImplementation(storageLayerAggregateWithoutProject())
                .matches(
                    logicalAggregate(
                        physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.COUNT
                                && agg.getCountArgumentExprIds().equals(
                                        ImmutableList.of(olapScan.getOutput().get(0).getExprId())))
                    )
                );

        // COUNT(*) still keeps a placeholder scan slot after column pruning, so its semantic
        // argument list must remain empty instead of being inferred from the physical scan shape.
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(), "count_star")),
                true, Optional.empty(), olapScan);
        context = MemoTestUtils.createCascadesContext(aggregate);

        PlanChecker.from(context)
                .applyImplementation(storageLayerAggregateWithoutProject())
                .matches(
                    logicalAggregate(
                        physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.COUNT
                                && agg.getCountArgumentExprIds().isEmpty())
                    )
                );

        // mix
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(olapScan.getOutput().get(0)), "count"),
                        new Alias(new Max(olapScan.getOutput().get(0)), "max")),
                true, Optional.empty(), olapScan);
        context = MemoTestUtils.createCascadesContext(aggregate);

        PlanChecker.from(context)
                .applyImplementation(storageLayerAggregateWithoutProject())
                .matches(
                    logicalAggregate(
                        physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.MIX)
                    )
                );
    }

    @Test
    public void testNullableFileCountUsesStorageLayerAggregate() {
        LogicalAggregate<LogicalFileScan> aggregate = newNullableFileCountAggregate();
        LogicalFileScan fileScan = aggregate.child();

        PlanChecker.from(MemoTestUtils.createCascadesContext(aggregate))
                .applyImplementation(storageLayerAggregateWithoutProjectForFileScan())
                .matches(logicalAggregate(
                        physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.COUNT
                                && agg.getCountArgumentExprIds().equals(
                                        ImmutableList.of(fileScan.getOutput().get(0).getExprId())))));
    }

    @Test
    public void testNullableFileCountDoesNotUseV1StorageLayerAggregate() {
        LogicalAggregate<LogicalFileScan> aggregate = newNullableFileCountAggregate();
        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.getConnectContext().getSessionVariable().enableFileScannerV2 = false;

        PlanChecker.from(context)
                .applyImplementation(storageLayerAggregateWithoutProjectForFileScan())
                .nonMatch(physicalStorageLayerAggregate());
    }

    @Test
    public void testMixedCountStarAndNullableFileCountDoesNotUseStorageLayerAggregate() {
        LogicalAggregate<LogicalFileScan> nullableCount = newNullableFileCountAggregate();
        LogicalFileScan fileScan = nullableCount.child();
        LogicalAggregate<LogicalFileScan> mixedCount = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(), "count_star"),
                        new Alias(new Count(fileScan.getOutput().get(0)), "count_nullable")),
                true, Optional.empty(), fileScan);

        PlanChecker.from(MemoTestUtils.createCascadesContext(mixedCount))
                .applyImplementation(storageLayerAggregateWithoutProjectForFileScan())
                .nonMatch(physicalStorageLayerAggregate());
    }

    @Test
    public void testPartitionValueMinMaxAndGrouping() {
        for (boolean projected : new boolean[] {false, true}) {
            for (boolean filtered : new boolean[] {false, true}) {
                LogicalFileScan scan = newPartitionFileScan(Optional.empty());
                Slot partition = scan.getOutput().get(1);
                Slot secondPartition = scan.getOutput().get(2);
                Plan child = partitionScanChild(scan, projected, filtered);
                checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(),
                        ImmutableList.of(new Alias(new Min(partition)), new Alias(new Max(partition))),
                        true, Optional.empty(), child), true, true);
                checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(partition),
                        ImmutableList.of(partition), true, Optional.empty(), child), true, true);
                checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(partition),
                        ImmutableList.of(partition, new Alias(new Max(secondPartition))),
                        true, Optional.empty(), child), true, true);
                checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(),
                        ImmutableList.of(new Alias(new Max(new IntegerLiteral(1)))),
                        true, Optional.empty(), child), true, true);
            }
        }
    }

    @Test
    public void testPartitionValueRejectsCardinalitySensitiveAggregates() {
        for (boolean projected : new boolean[] {false, true}) {
            for (boolean filtered : new boolean[] {false, true}) {
                LogicalFileScan scan = newPartitionFileScan(Optional.empty());
                Slot partition = scan.getOutput().get(1);
                Plan child = partitionScanChild(scan, projected, filtered);
                // Count(distinct) is deliberately absent here: it is duplicate-insensitive and is
                // covered by testPartitionValueSupportsCountDistinct.
                for (Expression function : ImmutableList.of(new Count(), new Count(partition),
                        new Sum(partition))) {
                    checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(),
                            ImmutableList.of(new Alias(function)), true, Optional.empty(), child), false, true);
                }
                checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(),
                        ImmutableList.of(new Alias(new Max(partition)), new Alias(new Count())),
                        true, Optional.empty(), child), false, true);
            }
        }
    }

    @Test
    public void testPartitionValueSupportsCountDistinct() {
        // COUNT(DISTINCT p) over a partition column is duplicate-insensitive: the scan emits one
        // row of partition values per file and every row of a file carries the same values, so
        // deduplicating that stream yields the same value set as deduplicating every row.
        for (boolean projected : new boolean[] {false, true}) {
            for (boolean filtered : new boolean[] {false, true}) {
                LogicalFileScan scan = newPartitionFileScan(Optional.empty());
                Slot partition = scan.getOutput().get(1);
                Plan child = partitionScanChild(scan, projected, filtered);
                checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(),
                        ImmutableList.of(new Alias(new Count(true, partition))), true, Optional.empty(),
                        child), true, true);
            }
        }
    }

    @Test
    public void testPartitionValueRejectsDataSlotsSampleAndDisabledCapability() {
        for (boolean projected : new boolean[] {false, true}) {
            for (boolean filtered : new boolean[] {false, true}) {
                LogicalFileScan scan = newPartitionFileScan(Optional.empty());
                Slot partition = scan.getOutput().get(1);
                LogicalFileScan fullScan = scan.withOperativeSlots(scan.getOutput());
                checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(partition),
                        ImmutableList.of(partition), true, Optional.empty(),
                        partitionScanChild(fullScan, projected, filtered)), false, true);
                LogicalFileScan sampled = newPartitionFileScan(Optional.of(new TableSample(50L, true, 7L)));
                checkPartitionValue(partitionMax(sampled, projected, filtered), false, true);
                checkPartitionValue(partitionMax(scan, projected, filtered), false, false);
                PluginDrivenExternalTable table = (PluginDrivenExternalTable) scan.getTable();
                Mockito.when(table.supportsPartitionValueOnly()).thenReturn(false);
                checkPartitionValue(partitionMax(scan, projected, filtered), false, true);
                Mockito.when(table.supportsPartitionValueOnly()).thenReturn(true);
                Mockito.when(table.getPartitionColumns(Mockito.any())).thenReturn(ImmutableList.of());
                checkPartitionValue(partitionMax(scan, projected, filtered), false, true);
            }
        }
    }

    @Test
    public void testPartitionValueRejectsVolatileAndNoneMovableExpressions() {
        for (boolean filtered : new boolean[] {false, true}) {
            LogicalFileScan scan = newPartitionFileScan(Optional.empty());
            Slot partition = scan.getOutput().get(1);
            Plan child = partitionScanChild(scan, false, filtered);
            List<Expression> unsafe = ImmutableList.of(new Add(partition, new Random()),
                    new AssertTrue(new GreaterThan(partition, new IntegerLiteral(0)), new VarcharLiteral("invalid")));
            for (Expression expression : unsafe) {
                checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(),
                        ImmutableList.of(new Alias(new Max(expression))), true, Optional.empty(), child), false, true);
                checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(expression),
                        ImmutableList.of(new Alias(expression)), true, Optional.empty(), child), false, true);
                Alias alias = new Alias(expression, "projected");
                LogicalProject<Plan> project = new LogicalProject<>(ImmutableList.of(alias), child);
                checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(),
                        ImmutableList.of(new Alias(new Max(alias.toSlot()))),
                        true, Optional.empty(), project), false, true);
            }
            Alias deterministic = new Alias(new Add(partition, new IntegerLiteral(1)), "projected");
            checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(),
                    ImmutableList.of(new Alias(new Max(deterministic.toSlot()))), true, Optional.empty(),
                    new LogicalProject<>(ImmutableList.of(deterministic), child)), true, true);
        }
        for (boolean projected : new boolean[] {false, true}) {
            LogicalFileScan scan = newPartitionFileScan(Optional.empty());
            Slot partition = scan.getOutput().get(1);
            for (Expression predicate : ImmutableList.of(new GreaterThan(new Random(), new IntegerLiteral(0)),
                    new AssertTrue(new GreaterThan(partition, new IntegerLiteral(0)), new VarcharLiteral("invalid")))) {
                Plan child = new LogicalFilter<>(ImmutableSet.of(predicate), scan);
                if (projected) {
                    child = new LogicalProject<>(ImmutableList.copyOf(scan.getOperativeSlots()), child);
                }
                checkPartitionValue(new LogicalAggregate<>(ImmutableList.of(),
                        ImmutableList.of(new Alias(new Max(partition))), true, Optional.empty(), child), false, true);
            }
        }
    }

    @Test
    public void testPartitionValueRequiresPrunedFilter() {
        LogicalFileScan scan = newPartitionFileScan(Optional.empty())
                .withSelectedPartitions(SelectedPartitions.NOT_PRUNED);
        checkPartitionValue(partitionMax(scan, false, true), false, true);
        checkPartitionValue(partitionMax(scan, true, true), false, true);
    }

    @Test
    public void testPartitionValuePropagatesMetadataErrors() {
        LogicalFileScan scan = newPartitionFileScan(Optional.empty());
        PluginDrivenExternalTable table = (PluginDrivenExternalTable) scan.getTable();
        Mockito.when(table.getPartitionColumns(Mockito.any())).thenThrow(new LinkageError("metadata failure"));
        Assertions.assertThrows(LinkageError.class,
                () -> checkPartitionValue(partitionMax(scan, false, false), true, true));
    }

    @Test
    public void testPartitionValueUsesScanReferenceSnapshot() {
        LogicalFileScan baseScan = newPartitionFileScan(Optional.empty());
        PluginDrivenExternalTable table = (PluginDrivenExternalTable) baseScan.getTable();
        TableSnapshot selector = TableSnapshot.versionOf("17");
        LogicalFileScan scan = new LogicalFileScan(new RelationId(2), table, baseScan.getQualifier(),
                baseScan.getOperativeSlots(), Optional.empty(), Optional.of(selector), Optional.empty(),
                Optional.of(baseScan.getOutput()));
        LogicalAggregate<Plan> aggregate = partitionMax(scan, false, false);
        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        MvccSnapshot snapshot = Mockito.mock(MvccSnapshot.class);
        StatementContext statement = Mockito.spy(context.getStatementContext());
        Mockito.doReturn(Optional.of(snapshot)).when(statement)
                .getSnapshot(table, Optional.of(selector), Optional.empty());
        context.getConnectContext().setStatementContext(statement);
        Mockito.when(table.getPartitionColumns(Optional.empty())).thenReturn(ImmutableList.of());
        Mockito.when(table.getPartitionColumns(Optional.of(snapshot))).thenReturn(
                ImmutableList.of(new Column("pi", Type.INT, false), new Column("p2", Type.INT, true)));
        PlanChecker.from(context).applyImplementation(storageLayerAggregateWithoutProjectForFileScan())
                .matches(physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.PARTITION_VALUE));
        Mockito.verify(table).getPartitionColumns(Optional.of(snapshot));
        Mockito.verify(table, Mockito.never()).getPartitionColumns(Optional.empty());
    }

    @Test
    public void testPartitionValueColumnNamesAreLocaleIndependent() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            checkPartitionValue(partitionMax(newPartitionFileScan(Optional.empty()), false, false), true, true);
        } finally {
            Locale.setDefault(original);
        }
    }

    private LogicalFileScan newPartitionFileScan(Optional<TableSample> sample) {
        PluginDrivenExternalTable table = (PluginDrivenExternalTable) newFileScan(Type.INT, false).getTable();
        List<Column> schema = ImmutableList.of(new Column("value", Type.INT, false),
                new Column("pI", Type.INT, false), new Column("p2", Type.INT, true));
        Mockito.when(table.getFullSchema()).thenReturn(schema);
        Mockito.when(table.getFullSchema(Mockito.any())).thenReturn(schema);
        Mockito.when(table.getPartitionColumns(Mockito.any())).thenReturn(
                ImmutableList.of(new Column("pi", Type.INT, false), schema.get(2)));
        Mockito.when(table.supportsPartitionValueOnly()).thenReturn(true);
        Mockito.when(table.initSelectedPartitions(Mockito.any()))
                .thenReturn(new SelectedPartitions(1, ImmutableMap.of(), true));
        LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                ImmutableList.of("catalog", "db"), ImmutableList.of(), sample,
                Optional.empty(), Optional.empty(), Optional.empty());
        return scan.withOperativeSlots(scan.getOutput().subList(1, 3));
    }

    private Plan partitionScanChild(LogicalFileScan scan, boolean projected, boolean filtered) {
        Plan child = scan;
        if (filtered) {
            child = new LogicalFilter<>(ImmutableSet.of(
                    new EqualTo(scan.getOutput().get(1), new IntegerLiteral(1))), child);
        }
        if (projected) {
            child = new LogicalProject<>(ImmutableList.copyOf(scan.getOperativeSlots()), child);
        }
        return child;
    }

    private LogicalAggregate<Plan> partitionMax(LogicalFileScan scan, boolean projected, boolean filtered) {
        return new LogicalAggregate<>(ImmutableList.of(),
                ImmutableList.of(new Alias(new Max(scan.getOutput().get(1)))), true, Optional.empty(),
                partitionScanChild(scan, projected, filtered));
    }

    private void checkPartitionValue(LogicalAggregate<? extends Plan> aggregate, boolean expected, boolean enabled) {
        Plan child = aggregate.child();
        boolean projected = child instanceof LogicalProject;
        if (projected) {
            child = child.child(0);
        }
        boolean filtered = child instanceof LogicalFilter;
        RuleType ruleType = filtered
                ? (projected ? RuleType.STORAGE_LAYER_PARTITION_VALUE_WITH_PROJECT_FILTER_FOR_FILE_SCAN
                        : RuleType.STORAGE_LAYER_PARTITION_VALUE_WITH_FILTER_FOR_FILE_SCAN)
                : (projected ? RuleType.STORAGE_LAYER_AGGREGATE_WITH_PROJECT_FOR_FILE_SCAN
                        : RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT_FOR_FILE_SCAN);
        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        org.apache.doris.qe.SessionVariable session = Mockito.spy(context.getConnectContext().getSessionVariable());
        Mockito.doReturn(enabled).when(session).isEnablePartitionColumnValueOnlyOptimization();
        context.getConnectContext().setSessionVariable(session);
        Rule rule = new AggregateStrategies().buildRules().stream()
                .filter(candidate -> candidate.getRuleType() == ruleType).findFirst().get();
        PlanChecker checker = PlanChecker.from(context).applyImplementation(rule);
        if (expected) {
            checker.matches(physicalStorageLayerAggregate()
                    .when(agg -> agg.getAggOp() == PushDownAggOp.PARTITION_VALUE));
        } else {
            checker.nonMatch(physicalStorageLayerAggregate()
                    .when(agg -> agg.getAggOp() == PushDownAggOp.PARTITION_VALUE));
        }
    }

    private LogicalAggregate<LogicalFileScan> newNullableFileCountAggregate() {
        LogicalFileScan fileScan = newFileScan(Type.INT, true);
        return new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(fileScan.getOutput().get(0)), "count")),
                true, Optional.empty(), fileScan);
    }

    private LogicalFileScan newFileScan(Type type, boolean nullable) {
        Column nullableColumn = new Column("value", type, nullable);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any()))
                .thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema()).thenReturn(ImmutableList.of(nullableColumn));
        // On this branch external file-scan tables are PluginDrivenExternalTable, so
        // LogicalFileScan.computeOutput() resolves the schema via the version-aware
        // getFullSchema(Optional<MvccSnapshot>) overload rather than the no-arg one.
        Mockito.when(table.getFullSchema(Mockito.any())).thenReturn(ImmutableList.of(nullableColumn));
        Mockito.when(table.getName()).thenReturn("nullable_file_table");
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        DatabaseIf<TableIf> database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(table.getDatabase()).thenReturn(database);
        return new LogicalFileScan(new RelationId(1), table,
                ImmutableList.of("catalog", "db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testFileMinMaxUnsafeCast() {
        for (boolean projected : new boolean[] {false, true}) {
            for (boolean nullable : new boolean[] {false, true}) {
                for (boolean strict : new boolean[] {false, true}) {
                    checkFileMinMaxCast(Type.BIGINT, IntegerType.INSTANCE, nullable, projected, strict, false);
                    checkFileMinMaxCast(Type.DOUBLE, IntegerType.INSTANCE, nullable, projected, strict, false);
                    checkFileMinMaxCast(DecimalV3Type.createDecimalV3Type(3, 2).toCatalogDataType(),
                            DecimalV3Type.createDecimalV3Type(2, 1), nullable, projected, strict, false);
                }
            }
        }
    }

    @Test
    public void testFileMinMaxSafeCast() {
        for (boolean projected : new boolean[] {false, true}) {
            for (boolean nullable : new boolean[] {false, true}) {
                checkFileMinMaxCast(Type.INT, BigIntType.INSTANCE, nullable, projected, false, true);
                checkFileMinMaxCast(Type.INT, DoubleType.INSTANCE, nullable, projected, false, true);
                checkFileMinMaxCast(DecimalV3Type.createDecimalV3Type(3, 2).toCatalogDataType(),
                        DecimalV3Type.createDecimalV3Type(4, 2), nullable, projected, false, true);
            }
        }
    }

    @Test
    public void testFileMinMaxFloatingCast() {
        for (boolean projected : new boolean[] {false, true}) {
            for (boolean nullable : new boolean[] {false, true}) {
                for (boolean strict : new boolean[] {false, true}) {
                    checkFileMinMaxCast(Type.DOUBLE, FloatType.INSTANCE, nullable, projected, strict, false);
                    checkFileMinMaxCast(Type.FLOAT, DoubleType.INSTANCE, nullable, projected, strict, false);
                    checkFileMinMaxCast(DecimalV3Type.createDecimalV3TypeNotCheck256(76, 60).toCatalogDataType(),
                            FloatType.INSTANCE, nullable, projected, strict, false);
                }
            }
        }
    }

    @Test
    public void testOlapMinMaxCast() {
        for (boolean projected : new boolean[] {false, true}) {
            for (boolean nullable : new boolean[] {false, true}) {
                for (boolean strict : new boolean[] {false, true}) {
                    checkOlapMinMaxCast(Type.BIGINT, IntegerType.INSTANCE, nullable, projected, strict, false);
                    checkOlapMinMaxCast(Type.DOUBLE, FloatType.INSTANCE, nullable, projected, strict, false);
                    checkOlapMinMaxCast(Type.INT, BigIntType.INSTANCE, nullable, projected, strict, true);
                }
            }
        }
    }

    private void checkOlapMinMaxCast(Type sourceType, DataType targetType, boolean nullable,
            boolean projected, boolean strict, boolean expectedPushdown) {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(1, "cast_table", 0);
        scan.getTable().getFullSchema().get(0).setType(sourceType);
        scan.getTable().getFullSchema().get(0).setIsAllowNull(nullable);
        checkMinMaxCast(scan, targetType, projected, strict, expectedPushdown);
    }

    private void checkFileMinMaxCast(Type sourceType, DataType targetType, boolean nullable,
            boolean projected, boolean strict, boolean expectedPushdown) {
        checkMinMaxCast(newFileScan(sourceType, nullable), targetType, projected, strict, expectedPushdown);
    }

    private void checkMinMaxCast(LogicalRelation scan, DataType targetType,
            boolean projected, boolean strict, boolean expectedPushdown) {
        Expression argument = new Cast(scan.getOutput().get(0), targetType, true, strict);
        Plan child = scan;
        RuleType ruleType = scan instanceof LogicalFileScan
                ? RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT_FOR_FILE_SCAN
                : RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT;
        if (projected) {
            Alias alias = new Alias(argument, "cast_value");
            child = new LogicalProject<>(ImmutableList.of(alias), scan);
            argument = alias.toSlot();
            ruleType = scan instanceof LogicalFileScan
                    ? RuleType.STORAGE_LAYER_AGGREGATE_WITH_PROJECT_FOR_FILE_SCAN
                    : RuleType.STORAGE_LAYER_AGGREGATE_WITH_PROJECT;
        }
        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(Collections.emptyList(),
                ImmutableList.of(new Alias(new Min(argument), "min"), new Alias(new Max(argument), "max")),
                true, Optional.empty(), child);
        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.getConnectContext().getSessionVariable().enableStrictCast = strict;
        RuleType selectedRuleType = ruleType;
        Rule rule = new AggregateStrategies().buildRules().stream()
                .filter(candidate -> candidate.getRuleType() == selectedRuleType).findFirst().get();
        PlanChecker checker = PlanChecker.from(context).applyImplementation(rule);
        if (expectedPushdown) {
            checker.matches(projected
                    ? logicalAggregate(logicalProject(physicalStorageLayerAggregate()))
                    : logicalAggregate(physicalStorageLayerAggregate()));
        } else {
            checker.nonMatch(physicalStorageLayerAggregate());
        }
    }

    @Override
    public RulePromise defaultPromise() {
        return RulePromise.IMPLEMENT;
    }

    @Test
    public void testWithProject() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(1, "tbl", 0);
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(olapScan.getOutput().get(0)), olapScan);
        LogicalAggregate<LogicalProject<LogicalOlapScan>> aggregate;
        CascadesContext context;

        // min max
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Min(project.getOutput().get(0)), "min")),
                true, Optional.empty(), project);
        context = MemoTestUtils.createCascadesContext(aggregate);

        PlanChecker.from(context)
                .applyImplementation(storageLayerAggregateWithProject())
                .matches(
                    logicalAggregate(
                        logicalProject(
                            physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.MIN_MAX)
                        )
                    )
                );

        // count
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(project.getOutput().get(0)), "count")),
                true, Optional.empty(), project);
        context = MemoTestUtils.createCascadesContext(aggregate);

        PlanChecker.from(context)
                .applyImplementation(storageLayerAggregateWithProject())
                .matches(
                    logicalAggregate(
                        logicalProject(
                            physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.COUNT
                                    && agg.getCountArgumentExprIds().equals(
                                            ImmutableList.of(olapScan.getOutput().get(0).getExprId())))
                        )
                    )
                );

        // mix
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(project.getOutput().get(0)), "count"),
                        new Alias(new Max(olapScan.getOutput().get(0)), "max")),
                true, Optional.empty(),
                project);
        context = MemoTestUtils.createCascadesContext(aggregate);

        PlanChecker.from(context)
                .applyImplementation(storageLayerAggregateWithProject())
                .matches(
                    logicalAggregate(
                        logicalProject(
                            physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.MIX)
                        )
                    )
                );
    }

    @Test
    public void testCountOnIndexRejectsIsNullOnProjectedCountSlot() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(2, "count_alias", 0);
        Index invertedIndex = new Index(1L, "idx_name", ImmutableList.of("name"),
                IndexType.INVERTED, null, "");
        olapScan.getTable().getIndexIdToMeta().values().forEach(
                meta -> meta.setIndexes(ImmutableList.of(invertedIndex)));

        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new IsNull(olapScan.getOutput().get(1))), olapScan);
        LogicalProject<LogicalFilter<LogicalOlapScan>> project = new LogicalProject<>(
                ImmutableList.of(new Alias(olapScan.getOutput().get(1), "x")), filter);
        LogicalAggregate<LogicalProject<LogicalFilter<LogicalOlapScan>>> aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(project.getOutput().get(0)), "count_x"),
                        new Alias(new Count(), "count_star")),
                true, Optional.empty(), project);
        CascadesContext context = MemoTestUtils.createCascadesContext(aggregate);
        context.getConnectContext().getSessionVariable().setEnablePushDownCountOnIndex(true);

        PlanChecker.from(context)
                .applyImplementation(countOnIndex())
                .matches(logicalAggregate(logicalProject(logicalFilter(logicalOlapScan()))));
    }

    @Test
    void testProjectionCheck() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(1, "tbl", 0);
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(new Alias(new Ln(olapScan.getOutput().get(0)), "alias")), olapScan);
        LogicalAggregate<LogicalProject<LogicalOlapScan>> aggregate;
        CascadesContext context;

        // min max
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Min(project.getOutput().get(0)), "min")),
                project);
        context = MemoTestUtils.createCascadesContext(aggregate);

        PlanChecker.from(context)
                .applyImplementation(storageLayerAggregateWithProject())
                .matches(
                    logicalAggregate(
                        logicalProject(
                            logicalOlapScan()
                        )
                    )
                );
    }

    private Rule storageLayerAggregateWithoutProject() {
        return new AggregateStrategies().buildRules()
                .stream()
                .filter(rule -> rule.getRuleType() == RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT)
                .findFirst()
                .get();
    }

    private Rule storageLayerAggregateWithoutProjectForFileScan() {
        return new AggregateStrategies().buildRules()
                .stream()
                .filter(rule -> rule.getRuleType()
                        == RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT_FOR_FILE_SCAN)
                .findFirst()
                .get();
    }

    private Rule storageLayerAggregateWithProject() {
        return new AggregateStrategies().buildRules()
                .stream()
                .filter(rule -> rule.getRuleType() == RuleType.STORAGE_LAYER_AGGREGATE_WITH_PROJECT)
                .findFirst()
                .get();
    }

    private Rule countOnIndex() {
        return new AggregateStrategies().buildRules()
                .stream()
                .filter(rule -> rule.getRuleType() == RuleType.COUNT_ON_INDEX)
                .findFirst()
                .get();
    }
}
