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


import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.horn.HornOptimizationContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecExecutionAny;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.RequireProperties;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;
import org.apache.horn4j.thrift.TDistributionSpec;
import org.apache.horn4j.thrift.TDorisHashSpec;
import org.apache.horn4j.thrift.TExpression;
import org.apache.horn4j.thrift.TFlattenedExpression;
import org.apache.horn4j.thrift.TOperator;
import org.apache.horn4j.thrift.TOperatorType;
import org.apache.horn4j.thrift.TOrderSpec;
import org.apache.horn4j.thrift.TPhysicalAgg;
import org.apache.horn4j.thrift.TPhysicalFilter;
import org.apache.horn4j.thrift.TPhysicalGrouping;
import org.apache.horn4j.thrift.TPhysicalJoin;
import org.apache.horn4j.thrift.TPhysicalLimit;
import org.apache.horn4j.thrift.TPhysicalMotion;
import org.apache.horn4j.thrift.TPhysicalProject;
import org.apache.horn4j.thrift.TPhysicalScan;
import org.apache.horn4j.thrift.TPhysicalSetOp;
import org.apache.horn4j.thrift.TPhysicalSort;
import org.apache.horn4j.thrift.TPhysicalTableScan;
import org.apache.horn4j.thrift.TPhysicalValueScan;
import org.apache.horn4j.thrift.TPhysicalWindow;
import org.apache.horn4j.thrift.TScalar;
import org.apache.horn4j.thrift.TSpecialHashSpecKind;
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

/**
 * Rebuild a Doris PhysicalPlan from a Horn TFlattenedExpression.
 *
 * <p>支持算子集：PhysicalTableScan / PhysicalFilter / PhysicalProject /
 * PhysicalMotion（Fragment boundary）。其它 physical 算子抛错由 caller fallback Nereids。
 *
 * <p>所有反译产生的 SlotReference 通过 {@link #scalarUidToSlot} 注册（key=horn 端
 * scalar_unique_id，全 plan tree 唯一），共享给 {@link DorisExpressionBuilder}
 * 反译 ColumnRef 时直接查表，保持上下游 plan 节点 slot 引用一致。
 */
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

        // SELECT 列序由 project_list 承载、被 horn PhysicalProject 保留，反译后
        // hornPhysicalPlan.getOutput() 即 SELECT 序，PhysicalResultSink.outputExprs 直接复用。
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

            // Doris PhysicalWindow 走外部排序模式：child 的 sort 由 PhysicalSort case 反译，
            // BE AnalyticSinkOperator 假设 input 已 sorted。kPhysicalInternalSortWindow
            // （window 内部自排）Doris 无对应算子，落 default 走 unsupported fallback。
            case kPhysicalWindow:
                plan = buildWindow(texpr, children.get(0));
                break;

            // grouping → Doris PhysicalRepeat。
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

            // 集合算子 → PhysicalUnion/Intersect/Except。distinct 已被 BuildAggForUnion
            // 降级成 Agg+UnionAll；传整个 children 列表（N 元，非 children.get(0)）。
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
        PhysicalPlan finalPlan = Horn2DorisUtils.setPhysicalProperties(texpr, plan);
        // 统一同步：plan 节点 ctor（outer join / agg 等）会用 withNullable 产生新 Slot 实例
        // （ExprId 不变、nullable 变），旧 slot 在 map 里过期会导致上游按 uid 直查拿错。
        // 在此按 ExprId 把 scalarUidToSlot 覆盖成 finalPlan.getOutput() 的真值。
        syncMapWithPlanOutput(finalPlan);
        return finalPlan;
    }

    /** 按 ExprId 把 finalPlan.getOutput() 的 slot 真值同步进 scalarUidToSlot。 */
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

    /**
     * Horn TPhysicalSetOp → Doris PhysicalUnion / Intersect / Except。三字段：
     *  - output_list → outputs：union 自身输出列（plain column ref → SlotReference，注册 uid）。
     *  - children_output_list → childrenOutputs：每孩子一组按列对齐 SlotReference。
     *  - children_const_expr_list → constantExprsList：常量行（literal，可空）。
     * 分布派生由 buildHelper 末尾 setPhysicalProperties 自动接管；logicalProperties 传 null → lazy。
     */
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

        // intersect/except 恒 DISTINCT、无 const-expr，分布由 cc 端 enforce 全列 hash。
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

        // union all：constantExprsList（可空，透传）。常量行经 translate 后是 literal/cast
        // （Alias 已在 forward flattenPreOrder 剥掉），非 NamedExpression，故统一包 Alias。
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
        // Doris 原生行为（scan output 永远全列），保证 buildScanDistributionSpec 的 hash
        // 列凑齐、DataPartition 构造不空。
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
        // RANDOM 分布表（无分布列可补）的 count(*) 场景 scanSlots 仍空，会 NPE：
        // 跟 Doris 原生 ColumnPruning.pruneOutput 一致，selectMinimumColumn 选最小宽度列兜底。
        if (scanSlots.isEmpty()) {
            List<SlotReference> candidates = new ArrayList<>();
            for (Column col : olapTable.getBaseSchema()) {
                candidates.add(SlotReference.fromColumn(
                        stmtCtx.getNextExprId(), olapTable, col, qualifier));
            }
            scanSlots.add(ExpressionUtils.selectMinimumColumn(candidates));
        }
        // 分区集走 external_relation_id 查 forward 登记的 prunedPartitionIds，
        // 不再从 thrift selected_partition_ids 读。查不到/未 set 都是 bug，直接抛
        // ——不兜底全分区（否则 self-join 裁剪不同时静默用错分区集）。
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
        // 跟我们登记 scalarUidToSlot 的 ExprId 不一致，所以必须显式传 props 锁定。
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

    /**
     * horn PhysicalValueScan 反译为 Doris 物理叶子：
     * <ul>
     *   <li>0 行 → {@link PhysicalEmptyRelation}（schema 列由 output_list 重建）</li>
     *   <li>1 行 → {@link PhysicalOneRowRelation}（projects = Alias(lit, schemaName)，
     *       Alias.exprId 复用 schemaSlot.exprId 保证上层引用稳定）</li>
     * </ul>
     * <p>N 行不可达：Doris BindExpression 已把 InlineTable 拆成 OneRowRelation × N + Union。
     */
    private PhysicalPlan buildValueScan(TExpression texpr) throws UserException {
        // horn PhysicalValueScan::ToThrift 走 TPhysicalScan wrapper sub-struct
        // （跟 TPhysicalTableScan 同模式），不是 TOperatorUnion 直挂的 slot 14。
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

        // PhysicalOneRowRelation 主线没 override computeOutput，传 null 会抛 "Not support
        // compute output"，故显式构造 LogicalProperties + supplier 锁定 schemaSlots
        // （PhysicalEmptyRelation 为一致性也走同款）。
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
        // Alias 必须复用 schemaSlot.exprId，否则 OneRow 的 output ExprId 跟
        // scalarUidToSlot 登记的不一致，上层 Project/Filter 引用就断。
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
                // mark slot 特例：AS 别名投影时 mark slot 与别名共享 ExprId，直接透传会显示内部名 $c$1；
                // 对 MarkJoinSlotReference 包 Alias 恢复用户列名（仅 mark slot，普通列包 Alias 会多余 Project）。
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

        // GROUPING() 函数还原：Doris 不能在 Project 里算 Grouping()，故把含 GroupingScalarFunction
        // 的 Alias 下推进 child Repeat.outputExpressions（由 RepeatNode 原生物化），本 Project 对应
        // 列退化成透传 slot；这层 Project 保留作 GROUPING_ID 的桥（Repeat.getOutput() 不含 groupingId）。
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
        // → AbstractPlan 自动 LazyCompute(this::computeLogicalProperties)
        // → PhysicalProject.computeOutput() = projects.map(toSlot)
        return new PhysicalProject<>(projects, null, child);
    }

    /** Translate a Horn Motion node to a Doris PhysicalDistribute (Fragment boundary). */
    private PhysicalPlan buildMotion(TExpression texpr, PhysicalPlan child) throws UserException {
        TPhysicalMotion tmotion = texpr.getOp().getOp_union().getPhysical_motion();
        TDistributionSpec tspec = tmotion.getDistribution_spec();
        // Merge gather: horn PhysicalMotionGather 自带 sort_info 表示跨 fragment 多路归并，
        // 反译成 MERGE_SORT + PhysicalDistribute[Gather] 两层（不需包 LOCAL_SORT 占位）。
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

        // mark join（IN / EXISTS-OR 子查询）：重建 MarkJoinSlotReference + mark 谓词。
        // 必须是 MarkJoinSlotReference 子类，否则 isMarkJoin()=false、不 append mark 列。
        // slot 身份经 scalarUidToSlot[uid] 复用：首次见到 uid 新建并注册，上层 Filter 同 uid 复用。
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

        // mark join：传 null logicalProperties → 惰性 computeOutput() 会 append mark boolean；
        // 预建 props 不含 mark 会漏列。非 mark join 仍用预建 props（保持既有行为）。
        LogicalProperties props = markSlotRef.isPresent() ? null : new LogicalProperties(
                () -> JoinUtils.getJoinOutput(joinType, left, right),
                () -> DataTrait.EMPTY_TRAIT);
        // PhysicalProperties 由 buildHelper 末尾 attachProperties 统一处理
        // （join driving-side inherit 规则；见 attachProperties 注释）
        PhysicalPlan join;
        if (useHash) {
            join = new PhysicalHashJoin<>(joinType, hashConjuncts, otherConjuncts,
                    markConjuncts, new DistributeHint(DistributeType.NONE), markSlotRef,
                    props, left, right);
        } else {
            join = new PhysicalNestedLoopJoin<>(joinType, hashConjuncts, otherConjuncts,
                    markConjuncts, markSlotRef, props, left, right);
        }
        // outer join 输出侧 nullable 升格回写由 buildHelper 末尾 syncMapWithPlanOutput 接管。
        return join;
    }

    /**
     * Convert a Horn TDistributionSpec to a Doris DistributionSpec.
     * 专给 buildMotion 用，返回 motion 的 target 分布（scan 节点分布由 buildScan 单独构造）。
     * 仅处理 motion derive 可能 emit 的 5 种类型；require-only 标签（kAny/kNonSingleton）出现即 bug，直接 throw。
     */
    private DistributionSpec toDistributionSpec(TDistributionSpec tspec) {
        switch (tspec.getDistribution_type()) {
            case kSingleton:
                // motion gather 之后单点 → DistributionSpecGather。
                return DistributionSpecGather.INSTANCE;
            case kReplicated:
            case kUniversal:
                // kReplicated: Broadcast target，每 instance 拿全份。
                // kUniversal: horn "everywhere"（系统表/常量），Doris 无此概念，Replicated 功能等价。
                return DistributionSpecReplicated.INSTANCE;
            case kHashed: {
                // kHashed 一定带 hash_columns；每个 hash 列的 unique_id 一定已被
                // child plan 登记到 scalarUidToSlot 且是 SlotReference（直接 cast）。
                List<ExprId> hashExprIds = new ArrayList<>();
                for (TOperator hashCol : tspec.getHash_columns()) {
                    long hornScalarUid = Horn2DorisUtils.getRootScalar(hashCol).getScalar_unique_id().getUnique_id();
                    hashExprIds.add(((SlotReference) hornCtx.getScalarUidToSlot().get(hornScalarUid)).getExprId());
                }
                // 带 DorisHashSpec → 绑定目标表 storage 桶布局。shuffleType 按 is_natural 分：
                //   true（scan 原生分布）→ NATURAL；false（motion shuffle 产物）→ STORAGE_BUCKETED。
                // 三要素(tableId/baseIndexId/partitionIds)与 scan 侧 buildScanDistributionSpec 逐字段对齐。
                if (tspec.isSetSpecial_hash_spec()
                        && tspec.getSpecial_hash_spec().getKind() == TSpecialHashSpecKind.kDoris) {
                    TDorisHashSpec storageBucketSpec = tspec.getSpecial_hash_spec().getDoris_hash_spec();
                    long storageBucketTableId = storageBucketSpec.getTable_id();
                    OlapTable storageBucketTable = (OlapTable) Env.getCurrentInternalCatalog()
                            .getTableByTableId(storageBucketTableId);
                    ShuffleType shuffleType = storageBucketSpec.isIs_natural()
                            ? ShuffleType.NATURAL : ShuffleType.STORAGE_BUCKETED;
                    return new DistributionSpecHash(
                            hashExprIds, shuffleType,
                            storageBucketTableId,
                            storageBucketTable.getBaseIndexId(),
                            new LinkedHashSet<>(storageBucketSpec.getSelected_partition_ids()));
                }
                // 无 special(Empty)→ 普通运行时 hash 重分布 → EXECUTION_BUCKETED
                //   (BE HASH_PARTITIONED)。NATURAL 仅描述 scan 自带桶分布、不能作 shuffle target。
                return new DistributionSpecHash(hashExprIds, ShuffleType.EXECUTION_BUCKETED);
            }
            case kRandom:
                // PhysicalMotionRandom 主动 scramble 到任意节点。DistributionSpecExecutionAny
                // 是 Doris "必 shuffle 但 target 任意"的专用 spec，跟 random target 语义吻合。
                return DistributionSpecExecutionAny.INSTANCE;
            default:
                // kAny / kNonSingleton: horn 端 require-only 标签，motion derive
                //   emit 这俩 = horn bug。kSentinel: thrift 哨兵值，合法 plan
                //   不该带。其它未知值同样异常。
                throw new IllegalStateException(
                        "Unexpected horn motion distribution type on derive path: "
                                + tspec.getDistribution_type());
        }
    }

    // PhysicalSort 反译成 Doris LOCAL_SORT phase（单 fragment 内完整排序）。
    // 上方若有 PhysicalMotionGather 配套，buildMotion 会自动包 MERGE_SORT + Distribute[Gather]。
    private PhysicalPlan buildSort(TExpression texpr, PhysicalPlan child) throws UserException {
        TPhysicalSort tsort = texpr.getOp().getOp_union().getPhysical_sort();
        TOrderSpec sortInfo = tsort.getSort_info();
        if (sortInfo == null) {
            throw new UserException("Horn output: PhysicalSort missing sort_info");
        }
        List<OrderKey> orderKeys = buildOrderKeys(sortInfo);
        return new PhysicalQuickSort<>(orderKeys, SortPhase.LOCAL_SORT, null, child);
    }

    // PhysicalLimit 反译:
    //   带 order_spec → PhysicalTopN (Global→MERGE_SORT, Local→LOCAL_SORT)；若 child 是
    //     PhysicalQuickSort 则跳过它（Limit 自带 order_spec 已表达 sort+truncate）。
    //   无 order_spec → 1:1 PhysicalLimit
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

    /**
     * 反译 horn PhysicalHashAgg / PhysicalScalarAgg → Doris PhysicalHashAggregate。三态 phase 映射：
     *
     *   is_split | is_local | AggregateParam              | agg_funcs 形态     | 动作
     *   ---------+----------+-----------------------------+--------------------+---------
     *   true     | true     | (LOCAL,  INPUT_TO_BUFFER)   | 原函数 ScalarAggFn | 注册 map
     *   true     | false    | (GLOBAL, BUFFER_TO_RESULT)  | ColumnRef          | lookup map
     *   false    | *        | (GLOBAL, INPUT_TO_RESULT)   | 原函数             | 单层直出
     *
     * 第三态覆盖 CombineTwoAdjacentAggs（撤销 split）的产物；scalar agg 走同一路径。
     */
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

        // 循环里只拼 output 并首次绑定 uid → slot（供上游 buildFilter/buildProject 按 uid 命中）。
        // ctor 的 adjustNullableForOutputs 会替换输出对象、改 nullable，交 buildHelper 末尾
        // syncMapWithPlanOutput 接管。scalarUidToAggFunc 的 fn 不在 plan.getOutput() 里，构造后单独刷新。
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
            // （local 节点 buildAgg 尾部已注册构造后真值，bottom-up 先于 global）----
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
                param, false /* maybeUsingStream */, null /* logicalProperties */, child);

        // nullable 升格回写由 buildHelper 末尾 syncMapWithPlanOutput 接管。
        // scalarUidToAggFunc 无需在此再注册：local 已 put OLD fn，global ctor 的
        // adjustNullableForOutputs 幂等补救（groupBy 空性一致 → fn 重建为等价新版）。
        return aggPlan;
    }

    /**
     * 反译 horn PhysicalWindow → Doris PhysicalWindow。
     * forward 已保证：analytic_functions 非空、共享同一 (partition, order, frame) spec、
     * window_frame 一定 emit，故本函数不再兜底空检查 / frame 默认值 / spec 拆组。
     * alias name 用 {@code fnExpr.toSql()}（TPhysicalWindow 不携带原始 SQL alias）；
     * isSkew 暂固定 false。
     */
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

        // frame 经 translateScalarNode 的 kWindowFrame case 分发（唯一返回 WindowFrame），直接强转。
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

        // 跟 Doris 主线一致：requireProperties 只是占位符，真正的 child 需求由
        // RequestPropertyDeriver.visitPhysicalWindow 从 partition/order 动态推导。
        RequireProperties requireProperties = RequireProperties.followParent();

        return new PhysicalWindow<>(
                wfg, requireProperties, windowExpressions, false /* isSkew */,
                null /* logicalProperties */, child);
    }

    /**
     * thrift kPhysicalGrouping → Doris PhysicalRepeat.
     * GROUPING_ID slot nullable=false to match Doris NormalizeRepeat / BE repeat_operator DCHECK;
     * other grouping_expr_list slots keep nullable as-is. col_to_grouping_col_map is built on BE,
     * not read here.
     */
    private PhysicalPlan buildRepeat(TExpression texpr, PhysicalPlan child) throws UserException {
        TPhysicalGrouping tg = texpr.getOp().getOp_union().getPhysical_grouping();

        // 1. grouping_set_list → List<List<Expression>>
        List<List<Expression>> groupingSets = new ArrayList<>(tg.getGrouping_set_list().size());
        for (List<TOperator> tset : tg.getGrouping_set_list()) {
            groupingSets.add(exprTranslator.translateList(tset));
        }

        // 2. grouping_id_func：单元素 list → SlotReference（fresh ExprId 由 forward 端 emit 保留）
        //    强制 nullable=false 对齐 BE repeat_operator 的 GROUPING_ID 非空契约。
        TOperator tGroupingIdFn = tg.getGrouping_id_func().get(0);
        SlotReference groupingId = (SlotReference) exprTranslator.translate(tGroupingIdFn);
        groupingId = groupingId.withNullable(false);
        long groupingIdUid = Horn2DorisUtils.getRootScalar(tGroupingIdFn)
                .getScalar_unique_id().getUnique_id();
        hornCtx.getScalarUidToSlot().put(groupingIdUid, groupingId);

        // 3. outputExpressions = grouping_expr_list ∪ aggregate_function_inputs
        //    每个 slot 注册到 scalarUidToSlot 让上层 group_by 引用命中；nullable 保留已有结果，
        //    backward 不再二次强制。
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

        // 4. PhysicalRepeat ctor（主线签名不含 col_to_grouping_col_map）；logicalProperties 传 null：
        //    buildHelper 末尾 setPhysicalProperties + syncMapWithPlanOutput 统一处理。
        return new PhysicalRepeat<>(
                groupingSets, outputExpressions, groupingId,
                null /* logicalProperties */, child);
    }

    /** 把 horn TOrderSpec.order_by_exprs 翻译成 Doris OrderKey 列表。 */
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
