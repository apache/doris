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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.implementation.AggregateStrategies;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ln;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
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
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

public class PhysicalStorageLayerAggregateTest implements MemoPatternMatchSupported {

    @Test
    public void testBinaryExtremaAreNotReadFromZoneMaps() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(1, "binary_values", 0);
        scan.getTable().getBaseSchema().get(0).setType(Type.VARBINARY);
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Max(scan.getOutput().get(0)), "max")),
                true, Optional.empty(), scan);
        // A truncated binary upper bound can be larger and shorter than every stored value.
        PlanChecker.from(MemoTestUtils.createCascadesContext(aggregate))
                .applyImplementation(storageLayerAggregateWithoutProject())
                .nonMatch(physicalStorageLayerAggregate());
    }

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

    private LogicalAggregate<LogicalFileScan> newNullableFileCountAggregate() {
        LogicalFileScan fileScan = newFileScan(Type.INT, true);
        return new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(fileScan.getOutput().get(0)), "count")),
                true, Optional.empty(), fileScan);
    }

    private LogicalFileScan newFileScan(Type type, boolean nullable) {
        Column nullableColumn = new Column("value", type, nullable);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any()))
                .thenReturn(SelectedPartitions.NOT_PRUNED);
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
        Expression argument = new Cast(scan.getOutput().get(0), targetType, true);
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
}
