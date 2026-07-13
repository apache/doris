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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.implementation.AggregateStrategies;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ln;
import org.apache.doris.nereids.trees.expressions.functions.table.Hdfs;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

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
                                && agg.shapeInfo().equals("PhysicalStorageLayerAggregate[tbl]"))
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
                            physicalStorageLayerAggregate().when(agg -> agg.getAggOp() == PushDownAggOp.COUNT)
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

    @Test
    void testNullableTvfCountWithoutOriginalColumnPushesTargetColumnShape() throws AnalysisException {
        ExternalFileTableValuedFunction catalogFunction =
                Mockito.mock(ExternalFileTableValuedFunction.class);
        Column arrayColumn = new Column("arr", ArrayType.create(Type.STRING, true), true);
        FunctionGenTable table = new FunctionGenTable(1, "hdfs",
                TableIf.TableType.TABLE_VALUED_FUNCTION, ImmutableList.of(arrayColumn), catalogFunction);
        Hdfs function = new Hdfs(
                new Properties(Collections.singletonMap("format", "parquet"))) {
            @Override
            protected TableValuedFunctionIf toCatalogFunction() {
                return catalogFunction;
            }
        };
        Mockito.when(catalogFunction.getTable()).thenReturn(table);

        SlotReference arraySlot = new SlotReference(StatementScopeIdGenerator.newExprId(), "arr",
                DataType.fromCatalogType(arrayColumn.getType()), true, ImmutableList.of("hdfs"));
        LogicalTVFRelation tvf = new LogicalTVFRelation(
                new RelationId(1), function, ImmutableList.of()).withCachedOutputs(ImmutableList.of(arraySlot));
        LogicalAggregate<LogicalTVFRelation> aggregate = new LogicalAggregate<>(
                Collections.emptyList(), ImmutableList.of(new Alias(new Count(arraySlot), "count")),
                true, Optional.empty(), tvf);

        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().enableFileScannerV2 = true;
        CascadesContext context = MemoTestUtils.createCascadesContext(connectContext, aggregate);

        PlanChecker.from(context)
                .applyImplementation(storageLayerAggregateWithoutProjectForTvf())
                .matches(logicalAggregate(physicalStorageLayerAggregate().when(agg ->
                        agg.getRelation() instanceof PhysicalTVFRelation
                                && agg.getAggOp() == PushDownAggOp.COUNT_NON_NULL
                                && agg.getAggSlot().equals(Optional.of(arraySlot.getExprId())))));
    }

    private Rule storageLayerAggregateWithoutProject() {
        return new AggregateStrategies().buildRules()
                .stream()
                .filter(rule -> rule.getRuleType() == RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT)
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

    private Rule storageLayerAggregateWithoutProjectForTvf() {
        return new AggregateStrategies().buildRules()
                .stream()
                .filter(rule -> rule.getRuleType()
                        == RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT_FOR_TVF)
                .findFirst()
                .get();
    }
}
