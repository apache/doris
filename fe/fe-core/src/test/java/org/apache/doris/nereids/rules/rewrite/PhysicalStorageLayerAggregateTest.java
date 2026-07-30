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
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ln;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
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
        Column nullableColumn = new Column("value", Type.INT, true);
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
