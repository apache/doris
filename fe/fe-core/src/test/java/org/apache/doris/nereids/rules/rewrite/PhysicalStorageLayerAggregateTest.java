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
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.implementation.AggregateStrategies;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.TryCast;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ln;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
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
    public void testFileCountCastNullabilityControlsStorageLayerAggregate() {
        LogicalFileScan fileScan = newNullableFileCountAggregate().child();
        List<Cast> unsafeCasts = ImmutableList.of(
                new Cast(fileScan.getOutput().get(0), TinyIntType.INSTANCE),
                new TryCast(fileScan.getOutput().get(0), TinyIntType.INSTANCE));
        for (Cast cast : unsafeCasts) {
            LogicalAggregate<LogicalFileScan> aggregate = new LogicalAggregate<>(
                    Collections.emptyList(), ImmutableList.of(new Alias(new Count(cast), "count")),
                    true, Optional.empty(), fileScan);
            PlanChecker.from(MemoTestUtils.createCascadesContext(aggregate))
                    .applyImplementation(storageLayerAggregateWithoutProjectForFileScan())
                    .nonMatch(physicalStorageLayerAggregate());
        }

        List<Cast> safeCasts = ImmutableList.of(
                new Cast(fileScan.getOutput().get(0), BigIntType.INSTANCE),
                new TryCast(fileScan.getOutput().get(0), BigIntType.INSTANCE),
                new Cast(fileScan.getOutput().get(0), StringType.INSTANCE),
                new TryCast(fileScan.getOutput().get(0), StringType.INSTANCE));
        for (Cast cast : safeCasts) {
            LogicalAggregate<LogicalFileScan> aggregate = new LogicalAggregate<>(
                    Collections.emptyList(), ImmutableList.of(new Alias(new Count(cast), "count")),
                    true, Optional.empty(), fileScan);
            PlanChecker.from(MemoTestUtils.createCascadesContext(aggregate))
                    .applyImplementation(storageLayerAggregateWithoutProjectForFileScan())
                    .matches(logicalAggregate(
                            physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.COUNT)));
        }
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
        Column nullableColumn = new Column("value", Type.INT, true);
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
        LogicalFileScan fileScan = new LogicalFileScan(new RelationId(1), table,
                ImmutableList.of("catalog", "db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        return new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(fileScan.getOutput().get(0)), "count")),
                true, Optional.empty(), fileScan);
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
    public void testCastThatMayProduceNullDoesNotUseStorageLayerAggregate() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(2, "cast_aggregate", 0);
        Cast cast = new Cast(olapScan.getOutput().get(0), TinyIntType.INSTANCE);
        TryCast tryCast = new TryCast(olapScan.getOutput().get(0), TinyIntType.INSTANCE);
        Cast stringCast = new Cast(olapScan.getOutput().get(1), TinyIntType.INSTANCE);
        TryCast stringTryCast = new TryCast(olapScan.getOutput().get(1), TinyIntType.INSTANCE);
        List<AggregateFunction> castAggregates = ImmutableList.of(
                new Count(cast), new Count(tryCast), new Min(cast), new Max(tryCast),
                new Count(stringCast), new Count(stringTryCast));

        for (AggregateFunction function : castAggregates) {
            LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                    Collections.emptyList(), ImmutableList.of(new Alias(function, "aggregate")),
                    true, Optional.empty(), olapScan);

            PlanChecker.from(MemoTestUtils.createCascadesContext(aggregate))
                    .applyImplementation(storageLayerAggregateWithoutProject())
                    .matches(logicalAggregate(logicalOlapScan()));
        }
    }

    @Test
    public void testSafeCastAggregateUsesStorageLayerAggregate() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(3, "safe_cast_aggregate", 0);
        Cast cast = new Cast(olapScan.getOutput().get(0), BigIntType.INSTANCE);
        TryCast tryCast = new TryCast(olapScan.getOutput().get(0), BigIntType.INSTANCE);
        List<AggregateFunction> castAggregates = ImmutableList.of(
                new Count(cast), new Count(tryCast), new Min(cast), new Max(tryCast));

        for (AggregateFunction function : castAggregates) {
            LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                    Collections.emptyList(), ImmutableList.of(new Alias(function, "aggregate")),
                    true, Optional.empty(), olapScan);

            PlanChecker.from(MemoTestUtils.createCascadesContext(aggregate))
                    .applyImplementation(storageLayerAggregateWithoutProject())
                    .matches(logicalAggregate(physicalStorageLayerAggregate()));
        }
    }

    @Test
    public void testCountAllowsNullPreservingNonNumericCast() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(4, "safe_non_numeric_count", 0);
        List<Cast> safeCasts = ImmutableList.of(
                new Cast(olapScan.getOutput().get(0), StringType.INSTANCE),
                new TryCast(olapScan.getOutput().get(0), StringType.INSTANCE));

        for (Cast cast : safeCasts) {
            LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                    Collections.emptyList(), ImmutableList.of(new Alias(new Count(cast), "count")),
                    true, Optional.empty(), olapScan);

            PlanChecker.from(MemoTestUtils.createCascadesContext(aggregate))
                    .applyImplementation(storageLayerAggregateWithoutProject())
                    .matches(logicalAggregate(
                            physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.COUNT)));
        }
    }

    @Test
    public void testMinMaxRejectsNullPreservingNonNumericCast() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(5, "non_order_preserving_cast", 0);
        Cast cast = new Cast(olapScan.getOutput().get(0), StringType.INSTANCE);
        TryCast tryCast = new TryCast(olapScan.getOutput().get(0), StringType.INSTANCE);
        List<AggregateFunction> castAggregates = ImmutableList.of(
                new Min(cast), new Max(cast), new Min(tryCast), new Max(tryCast));

        for (AggregateFunction function : castAggregates) {
            LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(
                    Collections.emptyList(), ImmutableList.of(new Alias(function, "aggregate")),
                    true, Optional.empty(), olapScan);

            PlanChecker.from(MemoTestUtils.createCascadesContext(aggregate))
                    .applyImplementation(storageLayerAggregateWithoutProject())
                    .matches(logicalAggregate(logicalOlapScan()));
        }
    }

    @Test
    public void testProjectedCastThatMayProduceNullDoesNotUseStorageLayerAggregate() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(3, "projected_cast_count", 0);
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(new Alias(
                        new Cast(olapScan.getOutput().get(0), TinyIntType.INSTANCE), "cast_value")),
                olapScan);
        LogicalAggregate<LogicalProject<LogicalOlapScan>> aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(project.getOutput().get(0)), "count")),
                true, Optional.empty(), project);

        PlanChecker.from(MemoTestUtils.createCascadesContext(aggregate))
                .applyImplementation(storageLayerAggregateWithProject())
                .matches(logicalAggregate(logicalProject(logicalOlapScan())));
    }

    @Test
    public void testProjectedNullPreservingNonNumericCastCountUsesStorageLayerAggregate() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(4, "projected_safe_cast_count", 0);
        List<Cast> safeCasts = ImmutableList.of(
                new Cast(olapScan.getOutput().get(0), StringType.INSTANCE),
                new TryCast(olapScan.getOutput().get(0), StringType.INSTANCE));
        for (Cast cast : safeCasts) {
            LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                    ImmutableList.of(new Alias(cast, "cast_value")), olapScan);
            LogicalAggregate<LogicalProject<LogicalOlapScan>> aggregate = new LogicalAggregate<>(
                    Collections.emptyList(),
                    ImmutableList.of(new Alias(new Count(project.getOutput().get(0)), "count")),
                    true, Optional.empty(), project);

            PlanChecker.from(MemoTestUtils.createCascadesContext(aggregate))
                    .applyImplementation(storageLayerAggregateWithProject())
                    .matches(logicalAggregate(logicalProject(physicalStorageLayerAggregate())));
        }
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
