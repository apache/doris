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

package org.apache.doris.horn.horn2doris;

import org.apache.doris.horn.HornOptimizationContext;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.horn4j.thrift.TDistributionSpec;
import org.apache.horn4j.thrift.TDorisHashSpec;
import org.apache.horn4j.thrift.TSpecialHashSpecKind;
import org.apache.horn4j.thrift.TExpression;
import org.apache.horn4j.thrift.TPhysicalMotion;
import org.apache.horn4j.thrift.TFlattenedExpression;
import org.apache.horn4j.thrift.TOperator;
import org.apache.horn4j.thrift.TOperatorType;
import org.apache.horn4j.thrift.TPhysicalFilter;
import org.apache.horn4j.thrift.TPhysicalJoin;
import org.apache.horn4j.thrift.TPhysicalAgg;
import org.apache.horn4j.thrift.TPhysicalLimit;
import org.apache.horn4j.thrift.TPhysicalProject;
import org.apache.horn4j.thrift.TPhysicalScan;
import org.apache.horn4j.thrift.TPhysicalSetOp;
import org.apache.horn4j.thrift.TPhysicalSort;
import org.apache.horn4j.thrift.TPhysicalValueScan;
import org.apache.horn4j.thrift.TPhysicalTableScan;
import org.apache.horn4j.thrift.TPhysicalWindow;
import org.apache.horn4j.thrift.TPhysicalGrouping;
import org.apache.horn4j.thrift.TOrderSpec;
import org.apache.horn4j.thrift.TScalar;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecExecutionAny;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequireProperties;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Rebuild a Doris PhysicalPlan from a Horn TFlattenedExpression */
public class DorisPhysicalPlanBuilder {

    private static final Logger LOG = LogManager.getLogger(DorisPhysicalPlanBuilder.class);

    private final HornOptimizationContext hornCtx;
    private final DorisExpressionBuilder exprTranslator;

    public DorisPhysicalPlanBuilder(HornOptimizationContext hornCtx) {
        this.hornCtx = hornCtx;
        this.exprTranslator = new DorisExpressionBuilder(hornCtx);
    }

    public PhysicalPlan build(TFlattenedExpression flatPlan) throws UserException {
        if (flatPlan == null || flatPlan.getTexprs() == null || flatPlan.getTexprs().isEmpty()) {
            throw new UserException("Horn CBO: empty optimized plan");
        }

        TExpression root = restoreTree(flatPlan, new AtomicInteger(0));

        // 默认 need_singleton=false
        PhysicalPlan hornPhysicalPlan = buildHelper(root);

        // forward 端 visitLogicalResultSink 把 sink emit 成 root LogicalProject
        List<NamedExpression> sinkOutputs = hornPhysicalPlan.getOutput().stream()
                .map(s -> (NamedExpression) s)
                .collect(Collectors.toList());
        return new PhysicalResultSink<>(
                sinkOutputs,
                Optional.empty(),
                hornPhysicalPlan.getLogicalProperties(),
                hornPhysicalPlan);
    }

    private TExpression restoreTree(TFlattenedExpression flat, AtomicInteger idx) {
        TExpression expr = flat.getTexprs().get(idx.getAndIncrement());
        long arity = expr.getArity();
        List<TExpression> children = new ArrayList<>();
        for (int i = 0; i < arity; i++) {
            children.add(restoreTree(flat, idx));
        }
        expr.setChildren(children);
        return expr;
    }

    private PhysicalPlan buildHelper(TExpression texpr) throws UserException {
        // Build children first
        List<PhysicalPlan> children = new ArrayList<>();
        if (texpr.getChildren() != null) {
            for (TExpression child : texpr.getChildren()) {
                children.add(buildHelper(child));
            }
        }

        TOperatorType opType = texpr.getOp().getOp_type();
        PhysicalPlan plan;

        switch (opType) {
            case kPhysicalTableScan:
                plan = buildScan(texpr);
                break;

            case kPhysicalValueScan:
                plan = buildValueScan(texpr);
                break;

            case kPhysicalFilter:
                plan = buildFilter(texpr, children.get(0));
                break;

            case kPhysicalProject:
                plan = buildProject(texpr, children.get(0));
                break;

            case kPhysicalSort:
                plan = buildSort(texpr, children.get(0));
                break;

            case kPhysicalLimit:
                plan = buildLimit(texpr, children.get(0));
                break;

            // group_by 非空 → kPhysicalHashAgg；空 → kPhysicalScalarAgg。同一套 buildAgg。
            case kPhysicalHashAgg:
            case kPhysicalScalarAgg:
                plan = buildAgg(texpr, children.get(0));
                break;

            // Doris Nereids PhysicalWindow 走外部排序模式：cascades 通过
            case kPhysicalWindow:
                plan = buildWindow(texpr, children.get(0));
                break;

            case kPhysicalGrouping:
                plan = buildRepeat(texpr, children.get(0));
                break;

            // Motion → PhysicalDistribute (Fragment boundary)
            case kPhysicalMotionGather:
            case kPhysicalMotionBroadcast:
            case kPhysicalMotionHashDistribute:
            case kPhysicalMotionRandom:
                plan = buildMotion(texpr, children.get(0));
                break;

            case kPhysicalInnerHashJoin:
            case kPhysicalInnerNestedLoopJoin:
            case kPhysicalLeftOuterHashJoin:
            case kPhysicalLeftOuterNestedLoopJoin:
            case kPhysicalRightOuterHashJoin:
            case kPhysicalRightOuterNestedLoopJoin:
            case kPhysicalFullOuterHashJoin:
            case kPhysicalFullOuterNestedLoopJoin:
            case kPhysicalLeftSemiHashJoin:
            case kPhysicalLeftSemiNestedLoopJoin:
            case kPhysicalRightSemiHashJoin:
            case kPhysicalRightSemiNestedLoopJoin:
            case kPhysicalLeftAntiSemiHashJoin:
            case kPhysicalLeftAntiSemiNestedLoopJoin:
            case kPhysicalRightAntiSemiHashJoin:
            case kPhysicalRightAntiSemiNestedLoopJoin:
            case kPhysicalNullAwareLeftAntiSemiHashJoin:
            case kPhysicalNullAwareLeftAntiSemiNestedLoopJoin:
            case kPhysicalCrossJoin:
                plan = buildJoin(texpr, children.get(0), children.get(1));
                break;

            // distinct 已被 Doris BuildAggForUnion 降级成 Agg+UnionAll，故只需 ALL 路径；
            case kPhysicalNarrayUnionAll:
            case kPhysicalUnionAll:
            case kPhysicalIntersect:
            case kPhysicalExcept:
                plan = buildSetOp(texpr, children);
                break;

            default:
                throw new UserException("Horn output: unsupported operator " + opType);
        }

        // 统一在末尾给 plan 设 PhysicalProperties + Statistics
        PhysicalPlan finalPlan = Horn2DorisUtils.setPhysicalProperties(texpr, plan, hornCtx);
        // 统一同步：把 scalarUidToSlot 中 ExprId 与 finalPlan.getOutput() 重合的
        syncMapWithPlanOutput(finalPlan);
        return finalPlan;
    }

    /** 按 ExprId 把 finalPlan.getOutput() 的 slot 真值同步进 scalarUidToSlot */
    private void syncMapWithPlanOutput(PhysicalPlan finalPlan) {
        if (hornCtx.getScalarUidToSlot().isEmpty()) {
            return;
        }
        List<Slot> output = finalPlan.getOutput();
        if (output.isEmpty()) {
            return;
        }
        Map<ExprId, Slot> byId = new HashMap<>(output.size());
        for (Slot s : output) {
            byId.put(s.getExprId(), s);
        }
        hornCtx.getScalarUidToSlot().replaceAll((uid, slot) -> {
            Slot fresh = byId.get(slot.getExprId());
            return fresh != null ? fresh : slot;
        });
    }

    /** Horn TPhysicalSetOp（op_type=kPhysicalNarrayUnionAll / kPhysicalUnionAll）→ Doris */
    private PhysicalPlan buildSetOp(TExpression texpr, List<PhysicalPlan> children) throws UserException {
        TPhysicalSetOp tsetop = texpr.getOp().getOp_union().getPhysical_set_op();

        // outputs：union 自身输出列（plain column ref → SlotReference，translate 内部注册 uid→slot）。
        List<NamedExpression> outputs = new ArrayList<>();
        for (Expression e : exprTranslator.translateList(tsetop.getOutput_list())) {
            outputs.add((NamedExpression) e);
        }

        // childrenOutputs：每孩子一组 SlotReference（按列对齐 output，位置映射契约）。
        List<List<SlotReference>> childrenOutputs = new ArrayList<>();
        for (List<TOperator> oneChild : tsetop.getChildren_output_list()) {
            List<SlotReference> cols = new ArrayList<>();
            for (Expression e : exprTranslator.translateList(oneChild)) {
                cols.add((SlotReference) e);
            }
            childrenOutputs.add(cols);
        }

        List<Plan> planChildren = new ArrayList<>(children);

        TOperatorType opType = texpr.getOp().getOp_type();
        if (opType == TOperatorType.kPhysicalIntersect || opType == TOperatorType.kPhysicalExcept) {
            List<NamedExpression> aligned = Horn2DorisUtils.alignSetOpOutputNullable(outputs, childrenOutputs);
            if (opType == TOperatorType.kPhysicalIntersect) {
                return new PhysicalIntersect(Qualifier.DISTINCT, aligned, childrenOutputs,
                        null /* logicalProperties → lazy */, planChildren);
            }
            return new PhysicalExcept(Qualifier.DISTINCT, aligned, childrenOutputs,
                    null /* logicalProperties → lazy */, planChildren);
        }

        // union all：constantExprsList（可空，透传）
        List<List<NamedExpression>> constExprs = new ArrayList<>();
        if (tsetop.isSetChildren_const_expr_list()) {
            for (List<TOperator> row : tsetop.getChildren_const_expr_list()) {
                List<NamedExpression> r = new ArrayList<>();
                for (Expression e : exprTranslator.translateList(row)) {
                    r.add(new Alias(e));
                }
                constExprs.add(r);
            }
        }
        return new PhysicalUnion(Qualifier.ALL, outputs, childrenOutputs, constExprs,
                null /* logicalProperties → lazy computeOutput */, planChildren);
    }

    private PhysicalPlan buildScan(TExpression texpr) throws UserException {
        TPhysicalScan physicalScan = texpr.getOp().getOp_union().getPhysical_scan();
        TPhysicalTableScan tableScan = physicalScan.getPhysical_table_scan();
        String tableName = tableScan.getTable_name();
        String dbName = tableScan.getDatabase_name();

        // horn 只处理 OlapTable
        OlapTable olapTable = (OlapTable) hornCtx.getCascadesContext().getConnectContext()
                .getCurrentCatalog()
                .getDbOrAnalysisException(dbName)
                .getTableOrAnalysisException(tableName);

        // 从 Horn output_list 构造 scan slots，并添加到 scalarUidToSlot。
        StatementContext stmtCtx = hornCtx.getCascadesContext().getStatementContext();
        ImmutableList<String> qualifier = ImmutableList.of(dbName, tableName);
        List<Slot> scanSlots = new ArrayList<>();
        if (texpr.getOutput_list() != null) {
            for (TOperator outOp : texpr.getOutput_list()) {
                TScalar scalar = Horn2DorisUtils.getRootScalar(outOp);
                String colName = scalar.getScalar_union().getScalar_column_ref().getColumn_name();
                SlotReference slotRef = SlotReference.fromColumn(
                        stmtCtx.getNextExprId(), olapTable, olapTable.getColumn(colName), qualifier);
                hornCtx.getScalarUidToSlot().put(scalar.getScalar_unique_id().getUnique_id(), slotRef);
                scanSlots.add(slotRef);
            }
        }
        // 分布列被裁剪时（如 count(*) / GROUP BY 非分布键）补回 scanSlots —— 对齐
        DistributionInfo scanDistInfo = olapTable.getDefaultDistributionInfo();
        if (scanDistInfo instanceof HashDistributionInfo) {
            for (Column distCol : ((HashDistributionInfo) scanDistInfo).getDistributionColumns()) {
                if (scanSlots.stream().noneMatch(
                        s -> s.getName().equalsIgnoreCase(distCol.getName()))) {
                    scanSlots.add(SlotReference.fromColumn(
                            stmtCtx.getNextExprId(), olapTable, distCol, qualifier));
                }
            }
        }
        // RANDOM 分布表（无分布列可补）的 count(*) 场景 scanSlots 仍空：实测删掉本
        if (scanSlots.isEmpty()) {
            List<SlotReference> candidates = new ArrayList<>();
            for (Column col : olapTable.getBaseSchema()) {
                candidates.add(SlotReference.fromColumn(
                        stmtCtx.getNextExprId(), olapTable, col, qualifier));
            }
            scanSlots.add(ExpressionUtils.selectMinimumColumn(candidates));
        }
        // 分区集走 external_relation_id 查 OptimizeContext 里 forward 登记的 prunedPartitionIds,
        if (!tableScan.isSetExternal_relation_id()) {
            throw new UserException("horn buildScan: TPhysicalTableScan missing external_relation_id (table "
                    + tableName + ")");
        }
        long extRelationId = tableScan.getExternal_relation_id();
        List<Long> partitionIds = hornCtx.getScanPrunedPartitions(extRelationId);
        if (partitionIds == null) {
            throw new UserException("horn buildScan: no pruned-partition binding for external_relation_id="
                    + extRelationId + " (table " + tableName + ")");
        }
        List<Long> tabletIds = new ArrayList<>();
        for (Long partId : partitionIds) {
            olapTable.getPartition(partId).getBaseIndex().getTablets()
                    .forEach(t -> tabletIds.add(t.getId()));
        }

        // PhysicalCatalogRelation.computeOutput() 用 table.getBaseSchema() 重 ExprId，
        ImmutableList<Slot> immutableScanSlots = ImmutableList.copyOf(scanSlots);
        LogicalProperties scanProps = new LogicalProperties(
                () -> immutableScanSlots,
                () -> DataTrait.EMPTY_TRAIT);

        DistributionSpec scanDistSpec = Horn2DorisUtils.buildScanDistributionSpec(
                olapTable, scanSlots, partitionIds);

        PhysicalOlapScan scan = new PhysicalOlapScan(
                hornCtx.getCascadesContext().getStatementContext().getNextRelationId(),
                olapTable,
                ImmutableList.of(dbName, tableName),
                olapTable.getBaseIndexId(),
                tabletIds,
                partitionIds,
                scanDistSpec,
                PreAggStatus.on(),
                immutableScanSlots,
                Optional.empty(),
                scanProps,
                Optional.empty(),
                immutableScanSlots,
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Optional.empty()
        );

        return scan;
    }

    /** horn PhysicalValueScan 反译为 Doris 物理叶子： */
    private PhysicalPlan buildValueScan(TExpression texpr) throws UserException {
        // horn PhysicalValueScan::ToThrift 走 TPhysicalScan wrapper sub-struct
        TPhysicalValueScan valueScan = texpr.getOp().getOp_union()
                .getPhysical_scan().getPhysical_value_scan();
        StatementContext stmtCtx = hornCtx.getCascadesContext().getStatementContext();

        List<TOperator> schemaColumns = valueScan.isSetOutput_list()
                ? valueScan.getOutput_list() : Collections.emptyList();
        if (schemaColumns.isEmpty()) {
            throw new UserException(
                    "Horn output: PhysicalValueScan missing output_list (schema)");
        }

        List<Expression> schemaExprs = exprTranslator.translateList(schemaColumns);
        List<Slot> schemaSlots = new ArrayList<>(schemaExprs.size());
        for (Expression e : schemaExprs) {
            schemaSlots.add((Slot) e);
        }

        List<List<TOperator>> rows = valueScan.isSetValue_lists()
                ? valueScan.getValue_lists() : Collections.emptyList();

        // PhysicalOneRowRelation 在主线没 override computeOutput → 传 null logicalProperties
        ImmutableList<Slot> immutableSchema = ImmutableList.copyOf(schemaSlots);
        LogicalProperties relationProps = new LogicalProperties(
                () -> immutableSchema,
                () -> DataTrait.EMPTY_TRAIT);

        if (rows.isEmpty()) {
            return new PhysicalEmptyRelation(stmtCtx.getNextRelationId(), schemaSlots, relationProps);
        }
        if (rows.size() > 1) {
            throw new UserException("Horn output: PhysicalValueScan with "
                    + rows.size() + " rows not supported");
        }
        // 1 行：PhysicalOneRowRelation.output 直接从 projects.toSlot 派生，
        List<TOperator> singleRow = rows.get(0);
        if (singleRow.size() != schemaSlots.size()) {
            throw new UserException("Horn output: PhysicalValueScan row arity "
                    + singleRow.size() + " != schema arity " + schemaSlots.size());
        }
        List<NamedExpression> projects = new ArrayList<>(schemaSlots.size());
        for (int i = 0; i < schemaSlots.size(); i++) {
            Slot slot = schemaSlots.get(i);
            Expression cellExpr = exprTranslator.translate(singleRow.get(i));
            projects.add(new Alias(slot.getExprId(), cellExpr, slot.getName()));
        }
        return new PhysicalOneRowRelation(stmtCtx.getNextRelationId(), projects, relationProps);
    }

    private PhysicalPlan buildFilter(TExpression texpr, PhysicalPlan child) throws UserException {
        TPhysicalFilter tfilter = texpr.getOp().getOp_union().getPhysical_filter();

        Set<Expression> conjuncts = new HashSet<>();
        for (TOperator predOp : tfilter.getFilter_list()) {
            Expression expr = exprTranslator.translate(predOp);
            LOG.debug("HORN_FILTER predicate: {}", expr);
            conjuncts.add(expr);
        }
        return new PhysicalFilter<>(conjuncts, null, child);
    }

    private PhysicalPlan buildProject(TExpression texpr, PhysicalPlan child) throws UserException {
        TPhysicalProject tproj = texpr.getOp().getOp_union().getPhysical_project();

        List<TOperator> projectList = tproj.getProject_list();
        List<TOperator> rewriteList = tproj.getRewrite_list();
        List<NamedExpression> projects = new ArrayList<>();
        for (int i = 0; i < projectList.size(); i++) {
            TOperator projOp = projectList.get(i);
            long projUid = Horn2DorisUtils.getRootScalar(projOp).getScalar_unique_id().getUnique_id();
            Slot existingSlot = hornCtx.getScalarUidToSlot().get(projUid);
            if (existingSlot != null) {
                // 已注册 slot 直接透传
                if (existingSlot instanceof MarkJoinSlotReference) {
                    String aliasName = Horn2DorisUtils.getRootScalar(rewriteList.get(i))
                            .getScalar_union().getScalar_column_ref().getColumn_name();
                    if (aliasName != null && !aliasName.equalsIgnoreCase(existingSlot.getName())) {
                        NamedExpression aliased = new Alias(existingSlot, aliasName);
                        projects.add(aliased);
                        hornCtx.getScalarUidToSlot().put(projUid, aliased.toSlot());
                        continue;
                    }
                }
                projects.add(existingSlot);
                continue;
            }
            Expression projExpr = exprTranslator.translate(projOp);
            String aliasName = Horn2DorisUtils.getRootScalar(rewriteList.get(i))
                    .getScalar_union().getScalar_column_ref().getColumn_name();
            NamedExpression namedProj = new Alias(projExpr, aliasName);
            projects.add(namedProj);
            hornCtx.getScalarUidToSlot().put(projUid, namedProj.toSlot());
        }

        // identity，且 PhysicalProject 里的 Grouping 翻译到 BE 会 "root is null"），故把含 GroupingScalarFunction 的
        if (child instanceof PhysicalRepeat
                && projects.stream().anyMatch(p -> p.containsType(GroupingScalarFunction.class))) {
            PhysicalRepeat<?> repeat = (PhysicalRepeat<?>) child;
            List<NamedExpression> repeatOutput = new ArrayList<>(repeat.getOutputExpressions());
            List<NamedExpression> newProjects = new ArrayList<>(projects.size());
            for (NamedExpression p : projects) {
                if (p.containsType(GroupingScalarFunction.class)) {
                    repeatOutput.add(p);              // Grouping(..) AS GROUPING_PREFIX_* 还原进 Repeat
                    newProjects.add(p.toSlot());      // Project 退化成透传该 slot
                } else {
                    newProjects.add(p);
                }
            }
            // Doris 计划不可变：withAggOutput 重建带新 output 的 Repeat，resetLogicalProperties 让 output 随新增列重算
            child = repeat.withAggOutput(repeatOutput).resetLogicalProperties();
            projects = newProjects;
        }

        // logicalProperties 传 null → AbstractPhysicalPlan ctor 转 Optional.empty()
        return new PhysicalProject<>(projects, null, child);
    }

    /** Translate a Horn Motion node to a Doris PhysicalDistribute (Fragment boundary) */
    private PhysicalPlan buildMotion(TExpression texpr, PhysicalPlan child) throws UserException {
        TPhysicalMotion tmotion = texpr.getOp().getOp_union().getPhysical_motion();
        TDistributionSpec tspec = tmotion.getDistribution_spec();
        // Merge gather: horn PhysicalMotionGather 自带 sort_info 时表示跨 fragment
        if (tmotion.isSetSort_info()
                && tmotion.getSort_info().getOrder_by_exprs() != null
                && !tmotion.getSort_info().getOrder_by_exprs().isEmpty()) {
            List<OrderKey> orderKeys = buildOrderKeys(tmotion.getSort_info());
            PhysicalPlan gather = new PhysicalDistribute<>(toDistributionSpec(tspec), child);
            return new PhysicalQuickSort<>(orderKeys, SortPhase.MERGE_SORT, null, gather);
        }
        // 普通 Motion (无 merge gather)：单纯 PhysicalDistribute。
        return new PhysicalDistribute<>(toDistributionSpec(tspec), child);
    }

    private PhysicalPlan buildJoin(TExpression texpr, PhysicalPlan left, PhysicalPlan right)
            throws UserException {
        TOperatorType opType = texpr.getOp().getOp_type();
        JoinType joinType = Horn2DorisUtils.getJoinType(opType);
        boolean useHash = Horn2DorisUtils.isHashJoin(opType);

        TPhysicalJoin tjoin = texpr.getOp().getOp_union().getPhysical_join();
        List<Expression> hashConjuncts = exprTranslator.translateList(tjoin.getEqual_join_predicate_list());
        List<Expression> otherConjuncts = exprTranslator.translateList(tjoin.getOther_join_conjuncts());

        // mark join（IN / EXISTS-OR 子查询）：重建 MarkJoinSlotReference + mark 谓词
        Optional<MarkJoinSlotReference> markSlotRef = Optional.empty();
        List<Expression> markConjuncts = ExpressionUtils.EMPTY_CONDITION;
        if (tjoin.isSetMark_slot() && !tjoin.getMark_slot().isEmpty()) {
            // mark_slot 是单元素 list（同 grouping_id_func）。
            TScalar markScalar = Horn2DorisUtils.getRootScalar(tjoin.getMark_slot().get(0));
            long markUid = markScalar.getScalar_unique_id().getUnique_id();
            Slot existing = hornCtx.getScalarUidToSlot().get(markUid);
            MarkJoinSlotReference mjsr;
            if (existing instanceof MarkJoinSlotReference) {
                mjsr = (MarkJoinSlotReference) existing;
            } else {
                String markName = markScalar.getScalar_union().getScalar_column_ref().getColumn_name();
                mjsr = new MarkJoinSlotReference(
                        hornCtx.getCascadesContext().getStatementContext().getNextExprId(),
                        markName, false);
                hornCtx.getScalarUidToSlot().put(markUid, mjsr);
            }
            markSlotRef = Optional.of(mjsr);
            if (tjoin.isSetMark_join_predicate_list()
                    && tjoin.getMark_join_predicate_list() != null
                    && !tjoin.getMark_join_predicate_list().isEmpty()) {
                markConjuncts = exprTranslator.translateList(tjoin.getMark_join_predicate_list());
            }
        }

        // mark join：传 null logicalProperties → AbstractPhysicalPlan 惰性走 computeOutput()，
        LogicalProperties props = markSlotRef.isPresent() ? null : new LogicalProperties(
                () -> JoinUtils.getJoinOutput(joinType, left, right),
                () -> DataTrait.EMPTY_TRAIT);
        // PhysicalProperties 由 buildHelper 末尾 attachProperties 统一处理
        PhysicalPlan join;
        if (useHash) {
            join = new PhysicalHashJoin<>(joinType, hashConjuncts, otherConjuncts,
                    markConjuncts, new DistributeHint(DistributeType.NONE), markSlotRef,
                    props, left, right);
        } else {
            join = new PhysicalNestedLoopJoin<>(joinType, hashConjuncts, otherConjuncts,
                    markConjuncts, markSlotRef, props, left, right);
        }
        // outer join 输出侧 nullable 升格的回写改由 buildHelper 末尾统一
        return join;
    }

    /** Convert a Horn TDistributionSpec to a Doris DistributionSpec */
    private DistributionSpec toDistributionSpec(TDistributionSpec tspec) {
        switch (tspec.getDistribution_type()) {
            case kSingleton:
                // motion gather 之后单点 → DistributionSpecGather。
                return DistributionSpecGather.INSTANCE;
            case kReplicated:
            case kUniversal:
                // kReplicated: PhysicalMotionBroadcast 的 target，每个 instance 都
                return DistributionSpecReplicated.INSTANCE;
            case kHashed: {
                // kHashed 一定带 hash_columns；每个 hash 列的 unique_id 一定已被
                List<ExprId> hashExprIds = new ArrayList<>();
                for (TOperator hashCol : tspec.getHash_columns()) {
                    long hornScalarUid = Horn2DorisUtils.getRootScalar(hashCol).getScalar_unique_id().getUnique_id();
                    hashExprIds.add(((SlotReference) hornCtx.getScalarUidToSlot().get(hornScalarUid)).getExprId());
                }
                // 带 DorisHashSpec 物理身份 → 绑定到目标表 storage 桶布局
                if (tspec.isSetSpecial_hash_spec()
                        && tspec.getSpecial_hash_spec().getKind() == TSpecialHashSpecKind.kDoris) {
                    TDorisHashSpec storageBucketSpec = tspec.getSpecial_hash_spec().getDoris_hash_spec();
                    long storageBucketTableId = storageBucketSpec.getTable_id();
                    OlapTable storageBucketTable = (OlapTable) Env.getCurrentInternalCatalog()
                            .getTableByTableId(storageBucketTableId);
                    return new DistributionSpecHash(
                            hashExprIds, ShuffleType.STORAGE_BUCKETED,
                            storageBucketTableId,
                            storageBucketTable.getBaseIndexId(),
                            new LinkedHashSet<>(storageBucketSpec.getSelected_partition_ids()));
                }
                // 无 special(Empty)→ 普通运行时 hash 重分布 → EXECUTION_BUCKETED
                return new DistributionSpecHash(hashExprIds, ShuffleType.EXECUTION_BUCKETED);
            }
            case kRandom:
                // PhysicalMotionRandom 主动 scramble 到运行时任意节点
                return DistributionSpecExecutionAny.INSTANCE;
            default:
                // kAny / kNonSingleton: horn 端 require-only 标签，motion derive
                throw new IllegalStateException(
                        "Unexpected horn motion distribution type on derive path: "
                                + tspec.getDistribution_type());
        }
    }

    // v7 PhysicalSort 反译：OrderEnforcer 兜底产的 sort enforcer，反译成 Doris
    private PhysicalPlan buildSort(TExpression texpr, PhysicalPlan child) throws UserException {
        TPhysicalSort tsort = texpr.getOp().getOp_union().getPhysical_sort();
        TOrderSpec sortInfo = tsort.getSort_info();
        if (sortInfo == null) {
            throw new UserException("Horn output: PhysicalSort missing sort_info");
        }
        List<OrderKey> orderKeys = buildOrderKeys(sortInfo);
        return new PhysicalQuickSort<>(orderKeys, SortPhase.LOCAL_SORT, null, child);
    }

    // v7 PhysicalLimit 反译:
    private PhysicalPlan buildLimit(TExpression texpr, PhysicalPlan child) {
        TPhysicalLimit tlimit = texpr.getOp().getOp_union().getPhysical_limit();
        long limit = tlimit.isSetLimit() ? tlimit.getLimit() : -1;
        long offset = tlimit.isSetOffset() ? tlimit.getOffset() : 0;
        boolean isGlobal = tlimit.isSetIs_global_limit() ? tlimit.isIs_global_limit() : true;
        if (tlimit.isSetOrder_spec()
                && tlimit.getOrder_spec().getOrder_by_exprs() != null
                && !tlimit.getOrder_spec().getOrder_by_exprs().isEmpty()) {
            List<OrderKey> orderKeys = buildOrderKeys(tlimit.getOrder_spec());
            SortPhase sortPhase = isGlobal ? SortPhase.MERGE_SORT : SortPhase.LOCAL_SORT;
            PhysicalPlan topnChild = child instanceof PhysicalQuickSort
                    ? (PhysicalPlan) ((PhysicalQuickSort<?>) child).child(0)
                    : child;
            return new PhysicalTopN<>(orderKeys, limit, offset, sortPhase, null, topnChild);
        }
        LimitPhase phase = isGlobal ? LimitPhase.GLOBAL : LimitPhase.LOCAL;
        return new PhysicalLimit<>(limit, offset, phase, null, child);
    }

    /** 反译 horn PhysicalHashAgg / PhysicalScalarAgg → Doris PhysicalHashAggregate */
    private PhysicalPlan buildAgg(TExpression texpr, PhysicalPlan child) throws UserException {
        TPhysicalAgg tagg = texpr.getOp().getOp_union().getPhysical_agg();
        boolean isSplit = tagg.isIs_split();
        boolean isLocal = tagg.isIs_local_agg();

        // group_by：列引用（极少表达式），反译后透传进 output（两阶段 group key slot 不变）
        List<Expression> groupByExprs = new ArrayList<>();
        List<NamedExpression> output = new ArrayList<>();
        if (tagg.getGroup_by_exprs() != null) {
            for (TOperator gbOp : tagg.getGroup_by_exprs()) {
                Expression gb = exprTranslator.translate(gbOp);
                groupByExprs.add(gb);
                if (gb instanceof NamedExpression) {
                    output.add((NamedExpression) gb);
                } else {
                    output.add(new Alias(gb));
                }
            }
        }

        // 注意：分支循环里只拼 output，不注册 map —— 注册统一在构造之后（见方法尾），
        AggregateParam param;
        if (isSplit && isLocal) {
            // ---- local: 原函数 → AggregateExpression(INPUT_TO_BUFFER) 包 Alias 出 buffer slot ----
            param = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
            if (tagg.getAggregate_functions() != null) {
                for (TOperator fnOp : tagg.getAggregate_functions()) {
                    long uid = Horn2DorisUtils.getRootScalar(fnOp).getScalar_unique_id().getUnique_id();
                    Expression fnExpr = exprTranslator.translate(fnOp);
                    if (!(fnExpr instanceof AggregateFunction)) {
                        throw new UserException("Horn: local agg expected AggregateFunction, got "
                                + fnExpr.getClass().getSimpleName());
                    }
                    AggregateFunction fn = (AggregateFunction) fnExpr;
                    Alias buffer = new Alias(new AggregateExpression(fn, param));
                    output.add(buffer);
                    hornCtx.getScalarUidToSlot().put(uid, buffer.toSlot());
                    hornCtx.getScalarUidToAggFunc().put(uid, fn);
                }
            }
        } else if (isSplit) {
            // ---- global: thrift 上是 ColumnRef，按 uid lookup 原函数 + buffer slot
            param = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
            if (tagg.getAggregate_functions() != null) {
                for (TOperator fnOp : tagg.getAggregate_functions()) {
                    long uid = Horn2DorisUtils.getRootScalar(fnOp).getScalar_unique_id().getUnique_id();
                    AggregateFunction fn = hornCtx.getScalarUidToAggFunc().get(uid);
                    Slot buffer = hornCtx.getScalarUidToSlot().get(uid);
                    if (fn == null || buffer == null) {
                        throw new UserException(
                                "Horn: global agg cannot find local fn/buffer for uid=" + uid);
                    }
                    Alias result = new Alias(new AggregateExpression(fn, param, buffer));
                    output.add(result);
                    hornCtx.getScalarUidToSlot().put(uid, result.toSlot());
                }
            }
        } else {
            // ---- 单层 (CombineTwoAdjacentAggs 撤销 split 或未拆): INPUT_TO_RESULT ----
            param = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT);
            if (tagg.getAggregate_functions() != null) {
                for (TOperator fnOp : tagg.getAggregate_functions()) {
                    long uid = Horn2DorisUtils.getRootScalar(fnOp).getScalar_unique_id().getUnique_id();
                    Expression fnExpr = exprTranslator.translate(fnOp);
                    if (!(fnExpr instanceof AggregateFunction)) {
                        throw new UserException("Horn: single agg expected AggregateFunction, got "
                                + fnExpr.getClass().getSimpleName());
                    }
                    AggregateFunction fn = (AggregateFunction) fnExpr;
                    Alias result = new Alias(new AggregateExpression(fn, param));
                    output.add(result);
                    hornCtx.getScalarUidToSlot().put(uid, result.toSlot());
                }
            }
        }

        PhysicalHashAggregate<PhysicalPlan> aggPlan = new PhysicalHashAggregate<>(
                groupByExprs, output, Optional.empty(),
                param, false /* maybeUsingStream */, null /* logicalProperties */,
                false /* hasSourceRepeat */, child);

        // scalarUidToSlot 的 nullable 升格回写改由 buildHelper 末尾统一
        return aggPlan;
    }

    /** 反译 horn PhysicalWindow → Doris PhysicalWindow */
    private PhysicalPlan buildWindow(TExpression texpr, PhysicalPlan child) throws UserException {
        TPhysicalWindow twindow = texpr.getOp().getOp_union().getPhysical_window();

        List<Expression> partitionKeys = twindow.isSetPartition_exprs()
                ? exprTranslator.translateList(twindow.getPartition_exprs())
                : ImmutableList.of();

        List<OrderExpression> orderKeys = new ArrayList<>();
        if (twindow.isSetOrder_by_exprs()) {
            for (OrderKey orderKey : buildOrderKeys(twindow.getOrder_by_exprs())) {
                orderKeys.add(new OrderExpression(orderKey));
            }
        }

        // frame 经 translateScalarNode 的 kWindowFrame case 统一分发（WindowFrame is-a
        WindowFrame frame = (WindowFrame) exprTranslator.translate(twindow.getWindow_frame().get(0));

        List<NamedExpression> windowExpressions = new ArrayList<>();
        for (TOperator fnOp : twindow.getAnalytic_functions()) {
            TScalar rootScalar = Horn2DorisUtils.getRootScalar(fnOp);
            long uid = rootScalar.getScalar_unique_id().getUnique_id();
            Expression fnExpr = exprTranslator.translate(fnOp);
            WindowExpression wExpr = new WindowExpression(
                    fnExpr, partitionKeys, orderKeys, frame, false /* isSkew */);
            Alias windowAlias = new Alias(wExpr, fnExpr.toSql());
            windowExpressions.add(windowAlias);
            hornCtx.getScalarUidToSlot().put(uid, windowAlias.toSlot());
        }

        WindowFrameGroup wfg = new WindowFrameGroup(windowExpressions.get(0));
        for (int i = 1; i < windowExpressions.size(); i++) {
            wfg.addGroup(windowExpressions.get(i));
        }

        // 跟 Doris 主线 LogicalWindowToPhysicalWindow.java:133 一致：
        RequireProperties requireProperties = RequireProperties.followParent();

        return new PhysicalWindow<>(
                wfg, requireProperties, windowExpressions, false /* isSkew */,
                null /* logicalProperties */, child);
    }

    /** <p>GROUPING_ID slot constructed nullable=false to match Doris NormalizeRepeat default */
    private PhysicalPlan buildRepeat(TExpression texpr, PhysicalPlan child) throws UserException {
        TPhysicalGrouping tg = texpr.getOp().getOp_union().getPhysical_grouping();

        // 1. grouping_set_list → List<List<Expression>>
        List<List<Expression>> groupingSets = new ArrayList<>(tg.getGrouping_set_list().size());
        for (List<TOperator> tset : tg.getGrouping_set_list()) {
            groupingSets.add(exprTranslator.translateList(tset));
        }

        // 2
        TOperator tGroupingIdFn = tg.getGrouping_id_func().get(0);
        SlotReference groupingId = (SlotReference) exprTranslator.translate(tGroupingIdFn);
        groupingId = groupingId.withNullable(false);
        long groupingIdUid = Horn2DorisUtils.getRootScalar(tGroupingIdFn)
                .getScalar_unique_id().getUnique_id();
        hornCtx.getScalarUidToSlot().put(groupingIdUid, groupingId);

        // 3
        List<NamedExpression> outputExpressions = new ArrayList<>();
        for (TOperator colOp : tg.getGrouping_expr_list()) {
            SlotReference slot = (SlotReference) exprTranslator.translate(colOp);
            outputExpressions.add(slot);
            long uid = Horn2DorisUtils.getRootScalar(colOp).getScalar_unique_id().getUnique_id();
            hornCtx.getScalarUidToSlot().put(uid, slot);
        }
        if (tg.isSetAggregate_function_inputs()) {
            for (TOperator colOp : tg.getAggregate_function_inputs()) {
                SlotReference slot = (SlotReference) exprTranslator.translate(colOp);
                outputExpressions.add(slot);
                long uid = Horn2DorisUtils.getRootScalar(colOp).getScalar_unique_id().getUnique_id();
                hornCtx.getScalarUidToSlot().put(uid, slot);
            }
        }

        // 4
        return new PhysicalRepeat<>(
                groupingSets, outputExpressions, groupingId,
                null /* logicalProperties */, child);
    }

    /** 把 horn TOrderSpec.order_by_exprs 翻译成 Doris OrderKey 列表 */
    private List<OrderKey> buildOrderKeys(TOrderSpec sortInfo) {
        List<OrderKey> orderKeys = new ArrayList<>();
        List<TOperator> exprList = sortInfo.getOrder_by_exprs();
        List<Boolean> ascList = sortInfo.getIs_asc_order();
        List<Boolean> nullsFirstList = sortInfo.getNulls_first();
        if (exprList == null) {
            return orderKeys;
        }
        for (int i = 0; i < exprList.size(); ++i) {
            Expression expr = exprTranslator.translate(exprList.get(i));
            boolean asc = i < ascList.size() ? ascList.get(i) : true;
            boolean nullsFirst = i < nullsFirstList.size() ? nullsFirstList.get(i) : !asc;
            orderKeys.add(new OrderKey(expr, asc, nullsFirst));
        }
        return orderKeys;
    }

    // getRootScalar 已移到 Horn2DorisUtils（跨 builder 复用：DorisExpressionBuilder.buildFrameBoundary 也需要）

}
