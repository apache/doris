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

package org.apache.doris.horn.doris2horn;


import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.horn.HornOptimizationContext;
import org.apache.doris.horn.horn2doris.Horn2DorisUtils;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.rules.rewrite.MultiDistinctFunctionStrategy;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.StatisticsCache;

import com.google.common.collect.Lists;
import org.apache.horn4j.thrift.TColumn;
import org.apache.horn4j.thrift.TColumnStats;
import org.apache.horn4j.thrift.TColumnValue;
import org.apache.horn4j.thrift.TDistributionSpec;
import org.apache.horn4j.thrift.TDistributionType;
import org.apache.horn4j.thrift.TDorisHashSpec;
import org.apache.horn4j.thrift.TExpression;
import org.apache.horn4j.thrift.TFlattenedExpression;
import org.apache.horn4j.thrift.TLogicalAgg;
import org.apache.horn4j.thrift.TLogicalFilter;
import org.apache.horn4j.thrift.TLogicalGrouping;
import org.apache.horn4j.thrift.TLogicalJoin;
import org.apache.horn4j.thrift.TLogicalLimit;
import org.apache.horn4j.thrift.TLogicalProject;
import org.apache.horn4j.thrift.TLogicalScan;
import org.apache.horn4j.thrift.TLogicalSetOp;
import org.apache.horn4j.thrift.TLogicalTableScan;
import org.apache.horn4j.thrift.TLogicalValueScan;
import org.apache.horn4j.thrift.TLogicalWindow;
import org.apache.horn4j.thrift.TOperator;
import org.apache.horn4j.thrift.TOperatorType;
import org.apache.horn4j.thrift.TOperatorUnion;
import org.apache.horn4j.thrift.TOrderSpec;
import org.apache.horn4j.thrift.TScanTableType;
import org.apache.horn4j.thrift.TSpecialHashSpec;
import org.apache.horn4j.thrift.TSpecialHashSpecKind;
import org.apache.horn4j.thrift.TTable;
import org.apache.horn4j.thrift.TTableStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Visitor: Doris LogicalPlan → Horn TFlattenedExpression。不支持的 Nereids 节点
 * 落 {@link #visit} throw，触发 fallback 回 Nereids 原生优化器。
 */
public class HornLogicalPlanThriftBuilder extends DefaultPlanVisitor<Void, List<TExpression>> {

    private static final org.apache.logging.log4j.Logger LOG =
            org.apache.logging.log4j.LogManager.getLogger(HornLogicalPlanThriftBuilder.class);

    private final HornOptimizationContext hornCtx;
    private final HornScalarThriftBuilder scalarTranslator;

    public HornLogicalPlanThriftBuilder(HornOptimizationContext hornCtx) {
        this.hornCtx = hornCtx;
        this.scalarTranslator = new HornScalarThriftBuilder();
    }

    /** Translate the whole LogicalPlan tree to TFlattenedExpression. */
    public TFlattenedExpression translate(LogicalPlan logicalPlan) {
        TFlattenedExpression result = new TFlattenedExpression();
        List<TExpression> texprs = new ArrayList<>();
        // sink 由 visitLogicalResultSink emit 成 identity Project，承载 SELECT 列序；
        // horn 不消除 Project，列序经 root LogicalProject 自然保留。
        logicalPlan.accept(this, texprs);
        result.setTexprs(texprs);
        return result;
    }

    @Override
    public Void visit(Plan plan, List<TExpression> texprs) {
        throw new UnsupportedOperationException(
                "Horn: unsupported Nereids node: " + plan.getClass().getSimpleName());
    }

    /** Append a TExpression for {@code plan}, then recurse into children pre-order. */
    private void emit(Plan plan, TOperatorType opType, TOperatorUnion opUnion,
                      List<TExpression> texprs) {
        TExpression texpr = new TExpression();
        TOperator top = new TOperator();
        top.setOp_type(opType);
        top.setOp_union(opUnion);
        texpr.setOp(top);
        texpr.setArity(plan.children().size());

        // FE-declared output column refs：每项是 kColumnRef TOperator（column_name +
        // table_name + external_slot_id + data_type），BE 按 external_slot_id 注册 slot_map。
        List<TOperator> outputList = new ArrayList<>();
        for (Slot slot : plan.getOutput()) {
            if (slot instanceof SlotReference) {
                outputList.add(scalarTranslator.visitSlotReference((SlotReference) slot, null));
            }
        }
        if (!outputList.isEmpty()) {
            texpr.setOutput_list(outputList);
        }

        texprs.add(texpr);

        for (Plan child : plan.children()) {
            child.accept(this, texprs);
        }
    }

    @Override
    public Void visitLogicalOlapScan(LogicalOlapScan scan, List<TExpression> texprs) {
        TOperatorUnion opUnion = new TOperatorUnion();
        TLogicalTableScan tscan = new TLogicalTableScan();
        OlapTable table = scan.getTable();

        // DEBUG: 检查 LogicalOlapScan 是否已做 partition pruning
        LOG.warn("HORN_FWD_SCAN table={} totalPartitions={} selectedPartitionIds={} selectedTabletIds.size={}",
                table.getName(), table.getPartitionIds().size(),
                scan.getSelectedPartitionIds(), scan.getSelectedTabletIds().size());

        tscan.setDatabase_name(scan.getQualifier().get(1));
        tscan.setTable_name(table.getName());
        tscan.setTable_alias(table.getName());
        tscan.setTable_type(TScanTableType.kTciTable);
        // schema_id 不设：BE 默认 -1 作通配符匹配任意 catalog entry，FE 不需要传。

        // scan_list：scan 自有输出列(含列裁剪)的 ColumnRef(带 external_slot_id)，
        // BE 直接用它而非 TExpression.output_list。
        List<TOperator> scanList = new ArrayList<>();
        for (Slot slot : scan.getOutput()) {
            if (slot instanceof SlotReference) {
                scanList.add(scalarTranslator.visitSlotReference((SlotReference) slot, null));
            }
        }
        tscan.setScan_list(scanList);

        // external_relation_id：FE per-scan opaque id（relationId），horn 只透传；
        // backward 用它重建分区/索引，self-join 两 scan relationId 不同可区分。
        long relationId = scan.getRelationId().asInt();
        tscan.setExternal_relation_id(relationId);
        hornCtx.putScanPrunedPartitions(relationId, new ArrayList<>(scan.getSelectedPartitionIds()));

        // emit base table 物理排序键（OLAP tablet 内按 key 列升序、nulls first）供消冗余 sort；
        // 只取 scan_list 中可见的 leading key prefix，遇第一个被裁掉的 key 列即停。
        KeysType keysType = table.getKeysType();
        if (keysType == KeysType.DUP_KEYS || keysType == KeysType.UNIQUE_KEYS
                || keysType == KeysType.AGG_KEYS) {
            // OLAP key 列物理恒为升序 + nulls first；某 key 列被裁剪则顺序在该列断开，停。
            List<OrderKey> keyOrderKeys = new ArrayList<>();
            for (Column keyCol : table.getBaseSchemaKeyColumns()) {
                SlotReference keySlot = findOutputSlotByColumn(scan.getOutput(), keyCol.getName());
                if (keySlot == null) {
                    break;
                }
                keyOrderKeys.add(new OrderKey(keySlot, /*isAsc=*/true, /*nullFirst=*/true));
            }
            if (!keyOrderKeys.isEmpty()) {
                tscan.setBase_table_order_spec(buildOrderSpec(keyOrderKeys));
            }
        }

        // scan distribution_spec：HASH 表 → kHashed + 分桶列 ColumnRef(保序) + bucket_num；
        // 其余 → kRandom。
        DistributionInfo distInfo = table.getDefaultDistributionInfo();
        TDistributionSpec distSpec = new TDistributionSpec();
        if (distInfo instanceof HashDistributionInfo) {
            distSpec.setDistribution_type(TDistributionType.kHashed);
            distSpec.setBucket_num(distInfo.getBucketNum());
            List<TOperator> hashColumns = new ArrayList<>();
            for (Column distCol : ((HashDistributionInfo) distInfo).getDistributionColumns()) {
                SlotReference distSlot = findOutputSlotByColumn(scan.getOutput(), distCol.getName());
                if (distSlot != null) {
                    hashColumns.add(scalarTranslator.visitSlotReference(distSlot, null));
                }
            }
            distSpec.setHash_columns(hashColumns);
            // colocate 物理身份：同一 stable colocate group 的 scan 拿同一 grpId（对齐判据），
            // 非 colocate 表 grpId = -1。
            ColocateTableIndex colocateIndex = Env.getCurrentColocateIndex();
            long colocateGroupId = -1;
            if (colocateIndex.isColocateTable(table.getId())
                    && !colocateIndex.isGroupUnstable(colocateIndex.getGroup(table.getId()))) {
                colocateGroupId = colocateIndex.getGroup(table.getId()).grpId;
            }
            // 整个 colocate 物理身份组装成 TDorisHashSpec 灌进 special_hash_spec，horn 还原成 DorisHashSpec。
            // is_natural：复用 buildScanDistributionSpec 判定（stable colocate group 或单分区为 NATURAL），
            // 纯标记，仅供反译标 shuffleType，不参与 horn cascade 匹配。
            DistributionSpec scanSpec = Horn2DorisUtils.buildScanDistributionSpec(
                    table, scan.getOutput(), new ArrayList<>(scan.getSelectedPartitionIds()));
            boolean isNatural = (scanSpec instanceof DistributionSpecHash)
                    && ((DistributionSpecHash) scanSpec).getShuffleType() == DistributionSpecHash.ShuffleType.NATURAL;
            TDorisHashSpec dorisHashSpec = new TDorisHashSpec();
            dorisHashSpec.setColocate_group_id(colocateGroupId);
            dorisHashSpec.setTable_id(table.getId());
            dorisHashSpec.setSelected_partition_ids(new ArrayList<>(scan.getSelectedPartitionIds()));
            dorisHashSpec.setIs_natural(isNatural);
            TSpecialHashSpec specialHashSpec = new TSpecialHashSpec();
            specialHashSpec.setKind(TSpecialHashSpecKind.kDoris);
            specialHashSpec.setDoris_hash_spec(dorisHashSpec);
            distSpec.setSpecial_hash_spec(specialHashSpec);
        } else {
            distSpec.setDistribution_type(TDistributionType.kRandom);
        }
        tscan.setDistribution_spec(distSpec);

        buildCatalog(table, tscan.getDatabase_name(), tscan.getTable_name());

        TLogicalScan tLogicalScan = new TLogicalScan();
        tLogicalScan.setLogical_table_scan(tscan);
        opUnion.setLogical_scan(tLogicalScan);
        emit(scan, TOperatorType.kLogicalTableGet, opUnion, texprs);
        return null;
    }

    @Override
    public Void visitLogicalOneRowRelation(LogicalOneRowRelation oneRow, List<TExpression> texprs) {
        // SELECT 1, 'x' → LogicalValueGet, value_lists = [[lit, ...]]
        buildValueGet(oneRow, java.util.Collections.singletonList(oneRow.getProjects()), texprs);
        return null;
    }

    @Override
    public Void visitLogicalEmptyRelation(LogicalEmptyRelation emptyRelation, List<TExpression> texprs) {
        // WHERE false / LIMIT 0 → LogicalValueGet, value_lists = []（0 行，schema 由 plan.getOutput() 抽）。
        buildValueGet(emptyRelation, java.util.Collections.emptyList(), texprs);
        return null;
    }

    /**
     * OneRow / Empty 两个常量源节点 → horn LogicalValueGet 共用 emit 路径。
     * rows：每行 {@code List<NamedExpression>}（cell 为 Alias(Literal) 或裸表达式，剥 Alias 译内层）；
     * schema 直接从 {@code plan.getOutput()} 抽。
     */
    private void buildValueGet(Plan plan, List<List<NamedExpression>> rows, List<TExpression> texprs) {
        TLogicalValueScan valueScan = new TLogicalValueScan();
        if (!rows.isEmpty()) {
            List<List<TOperator>> valueLists = new ArrayList<>(rows.size());
            for (List<NamedExpression> row : rows) {
                List<TOperator> rowCells = new ArrayList<>(row.size());
                for (NamedExpression cell : row) {
                    Expression literalExpr = Doris2HornUtils.unwrapExpr(cell);
                    rowCells.add(scalarTranslator.translate(literalExpr));
                }
                valueLists.add(rowCells);
            }
            valueScan.setValue_lists(valueLists);
        }
        List<TOperator> outputSchema = new ArrayList<>();
        for (Slot slot : plan.getOutput()) {
            if (slot instanceof SlotReference) {
                outputSchema.add(scalarTranslator.visitSlotReference((SlotReference) slot, null));
            }
        }
        valueScan.setOutput_list(outputSchema);

        TLogicalScan scanWrapper = new TLogicalScan();
        scanWrapper.setLogical_value_scan(valueScan);
        TOperatorUnion opUnion = new TOperatorUnion();
        opUnion.setLogical_scan(scanWrapper);
        emit(plan, TOperatorType.kLogicalValueGet, opUnion, texprs);
    }

    /**
     * 构建 TTable（schema + col/table stats）供 horn StaticMataProvider，并注册到
     * {@link HornOptimizationContext#addTable}。
     * <p>num_rows 规则：所有列都有真实 stats 才传 num_rows>0（stats-full 路径），任一列缺
     * stats 则 num_rows=-1（no-stats 降级，避免 StatisticsDerive 崩溃）；row count 取 colStat.count。
     * colStat unknown 的列不 set TColumn.col_stats（optional，unset 合法）。
     */
    private void buildCatalog(OlapTable table, String dbName, String tblName) {
        TTable ttable = new TTable();
        ttable.setDb_name(dbName);
        ttable.setTbl_name(tblName);

        // clustering_columns / virtual_columns / schema_id 三个字段 horn 端目前无消费者，
        // FE 不传无影响（schema_id 由 BE 用 -1 通配符匹配）；接入 partition pruning / schema
        // versioning 时再补。

        ConnectContext ctx = hornCtx.getCascadesContext().getConnectContext();
        StatisticsCache statsCache = Env.getCurrentEnv().getStatisticsCache();
        long catalogId = table.getDatabase().getCatalog().getId();
        long dbId = table.getDatabase().getId();
        long tblId = table.getId();
        long idxId = table.getBaseIndexId();

        List<Column> baseSchema = table.getBaseSchema();
        List<TColumn> columns = new ArrayList<>();
        boolean allColumnStatsAvailable = true;
        long tableRowCount = -1;
        for (int i = 0; i < baseSchema.size(); i++) {
            Column col = baseSchema.get(i);
            ColumnStatistic colStat = statsCache.getColumnStatistics(
                    catalogId, dbId, tblId, idxId, col.getName(), ctx);
            boolean columnStatsAvailable = colStat != null && colStat != ColumnStatistic.UNKNOWN;
            if (columnStatsAvailable && tableRowCount < 0) {
                tableRowCount = (long) colStat.count;
            } else if (!columnStatsAvailable) {
                allColumnStatsAvailable = false;
            }

            TColumn tcol = new TColumn();
            tcol.setColumnName(col.getName());
            tcol.setColumnType(DorisTypeToHornConverter.convertCatalogType(col.getType()));
            tcol.setPosition(i);
            if (columnStatsAvailable) {
                TColumnStats tcolStats = new TColumnStats();
                tcolStats.setAvg_size(colStat.avgSizeByte);
                tcolStats.setMax_size(col.getType().getLength());
                tcolStats.setNum_distinct_values((long) colStat.ndv);
                tcolStats.setNum_nulls((long) colStat.numNulls);
                TColumnValue low = new TColumnValue();
                low.setDouble_val(colStat.minValue);
                tcolStats.setLow_value(low);
                TColumnValue high = new TColumnValue();
                high.setDouble_val(colStat.maxValue);
                tcolStats.setHigh_value(high);
                tcol.setCol_stats(tcolStats);
            }
            columns.add(tcol);
        }
        ttable.setColumns(columns);

        TTableStats tableStats = new TTableStats();
        tableStats.setNum_rows(allColumnStatsAvailable ? tableRowCount : -1);
        ttable.setTable_stats(tableStats);

        // table_id / partition_num 不再传:都是 horn C++ 从不读的死字段。colocate 身份
        // (含 table_id)在 scan distribution_spec.special_hash_spec(TDorisHashSpec)里。
        // 详见 horn/docs/extended4/36 §9。
        hornCtx.addTable(ttable);
    }

    @Override
    public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, List<TExpression> texprs) {
        TOperatorUnion opUnion = new TOperatorUnion();
        TLogicalFilter tfilter = new TLogicalFilter();
        tfilter.setPredicate_list(
                scalarTranslator.translateList(new ArrayList<>(filter.getConjuncts())));
        opUnion.setLogical_filter(tfilter);
        emit(filter, TOperatorType.kLogicalSelect, opUnion, texprs);
        return null;
    }

    @Override
    public Void visitLogicalProject(LogicalProject<? extends Plan> project, List<TExpression> texprs) {
        buildProject(project, project.getProjects(), project.getOutput(), texprs);
        return null;
    }

    /**
     * LogicalResultSink → horn identity Project：sink.outputExprs 即 SELECT 列序（project_list），
     * sink.getOutput() 对应 rewrite_list；emit 后递归 sink.child(0)。
     */
    @Override
    public Void visitLogicalResultSink(LogicalResultSink<? extends Plan> sink, List<TExpression> texprs) {
        buildProject(sink, sink.getOutputExprs(), sink.getOutput(), texprs);
        return null;
    }

    /**
     * 把 plan 当 LogicalProject emit：projects 写 project_list、output 写 rewrite_list（限
     * SlotReference）。visitLogicalProject 与 visitLogicalResultSink 共用。
     */
    private void buildProject(Plan plan, List<NamedExpression> projects,
                                       List<Slot> output, List<TExpression> texprs) {
        TLogicalProject projectOp = new TLogicalProject();
        projectOp.setProject_list(scalarTranslator.translateList(projects));
        List<TOperator> rewriteList = new ArrayList<>();
        for (Slot slot : output) {
            if (slot instanceof SlotReference) {
                rewriteList.add(scalarTranslator.visitSlotReference((SlotReference) slot, null));
            }
        }
        projectOp.setRewrite_list(rewriteList);
        TOperatorUnion opUnion = new TOperatorUnion();
        opUnion.setLogical_project(projectOp);
        emit(plan, TOperatorType.kLogicalProject, opUnion, texprs);
    }

    /**
     * Doris LogicalAggregate → horn TLogicalAgg（单层 forward，horn cascades 的 XformSplitAgg
     * 自己拆 Local/Global）。HAVING 已被 RBO 上提为独立 LogicalFilter，不在此处理。
     */
    @Override
    public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, List<TExpression> texprs) {
        // distinct 处理：用 MultiDistinctFunctionStrategy.rewrite 把 count/sum(DISTINCT x) 转成
        // multi_distinct_count/sum(x)（其 BUFFER 天然支持两阶段，horn 当普通 agg fn 走 SplitAgg）；
        // 转换后仍残留 distinct 的（avg(DISTINCT) 等）fallback。
        if (hasDistinctAggFn(agg)) {
            LogicalAggregate<? extends Plan> converted = MultiDistinctFunctionStrategy.rewrite(agg);
            if (hasDistinctAggFn(converted)) {
                throw new UnsupportedOperationException(
                        "Horn: distinct agg fn not convertible to multi_distinct");
            }
            return converted.accept(this, texprs);
        }
        TLogicalAgg tagg = new TLogicalAgg();
        tagg.setGroup_by_exprs(scalarTranslator.translateList(agg.getGroupByExpressions()));
        // 从 outputExpressions 抽 AggregateFunction（通常包在 Alias 里）emit 成 TScalarAggFn；
        // agg_computed_cols 平行存对应输出列（Alias slot，带 name + ExprId），BE 按位对齐注册 slot_map。
        List<TOperator> aggFuncs = new ArrayList<>();
        List<TOperator> aggComputedCols = new ArrayList<>();
        for (NamedExpression out : agg.getOutputExpressions()) {
            Expression inner = Doris2HornUtils.unwrapExpr(out);
            if (inner instanceof AggregateFunction) {
                aggFuncs.add(scalarTranslator.translate(inner));
                aggComputedCols.add(scalarTranslator.visitSlotReference(
                        (SlotReference) out.toSlot(), null));
            }
        }
        tagg.setAggregate_functions(aggFuncs);
        tagg.setAgg_computed_cols(aggComputedCols);
        // FE 进来的 agg 恒未拆分；BE 据此把 agg_type_ 定成 kLocal，消 UB。
        tagg.setIs_split(false);
        TOperatorUnion opUnion = new TOperatorUnion();
        opUnion.setLogical_agg(tagg);
        emit(agg, TOperatorType.kLogicalAgg, opUnion, texprs);
        return null;
    }

    /**
     * Doris LogicalUnion → horn TLogicalSetOp（kLogicalNarrayUnionAll）。前提：UNION DISTINCT 已被
     * BuildAggForUnion 降级为 Agg + UnionAll，故 qualifier 恒 ALL，且经 MergeSetOperations 扁平成 N 元。
     * 字段：output_list 由 emit 设进 TExpression.output_list；children_output_list 每孩子按列对齐；
     * children_const_expr_list 为常量 SELECT 行（可空）。
     */
    @Override
    public Void visitLogicalUnion(LogicalUnion union, List<TExpression> texprs) {
        // 只支持 UNION ALL；distinct 必已被 BuildAggForUnion 降级。
        if (union.getQualifier() != Qualifier.ALL) {
            throw new UnsupportedOperationException(
                    "Horn: non-ALL union should have been lowered by BuildAggForUnion");
        }
        // 纯常量 union（0 孩子、仅 constantExprsList）暂不支持：cc 端 StatisticsDerive 对 0 孩子
        // SIGSEGV。fallback 回 Nereids。
        if (union.children().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Horn: constant-only union (no child) not supported (cc StatisticsDerive SIGSEGV)");
        }
        emitSetOp(union, TOperatorType.kLogicalNarrayUnionAll, texprs);
        return null;
    }

    /**
     * Doris LogicalIntersect → horn TLogicalSetOp（kLogicalIntersect）。MergeSetOperations 可合并成
     * N 元；intersect 恒 DISTINCT（Doris 绑定期拒 ALL），无 constantExprsList。
     */
    @Override
    public Void visitLogicalIntersect(LogicalIntersect intersect, List<TExpression> texprs) {
        // intersect 恒 DISTINCT（Doris 绑定期拒 ALL）；防御性断言，非 DISTINCT → fallback。
        if (intersect.getQualifier() != Qualifier.DISTINCT) {
            throw new UnsupportedOperationException(
                    "Horn: intersect must be DISTINCT (Doris rejects ALL at binding)");
        }
        emitSetOp(intersect, TOperatorType.kLogicalIntersect, texprs);
        return null;
    }

    /**
     * Doris LogicalExcept → horn TLogicalSetOp（kLogicalExcept）。不被 MergeSetOperations 合并，恒嵌套
     * 二元；恒 DISTINCT，无 constantExprsList。
     */
    @Override
    public Void visitLogicalExcept(LogicalExcept except, List<TExpression> texprs) {
        // except 恒 DISTINCT（Doris 绑定期拒 ALL）；防御性断言，非 DISTINCT → fallback。
        if (except.getQualifier() != Qualifier.DISTINCT) {
            throw new UnsupportedOperationException(
                    "Horn: except must be DISTINCT (Doris rejects ALL at binding)");
        }
        emitSetOp(except, TOperatorType.kLogicalExcept, texprs);
        return null;
    }

    /**
     * 所有 set op（UNION ALL / INTERSECT / EXCEPT）共用：建 TLogicalSetOp(children_output_list) → emit。
     * qualifier / empty-children guard 由各 visitX 自负；const-expr 仅 LogicalUnion 有。
     */
    private void emitSetOp(LogicalSetOperation setOp, TOperatorType opType,
                           List<TExpression> texprs) {
        TLogicalSetOp tsetop = new TLogicalSetOp();
        List<List<TOperator>> childrenOut = new ArrayList<>();
        for (List<SlotReference> oneChild : setOp.getRegularChildrenOutputs()) {
            childrenOut.add(scalarTranslator.translateList(oneChild));
        }
        tsetop.setChildren_output_list(childrenOut);
        if (setOp instanceof LogicalUnion) {
            List<List<NamedExpression>> constExprsList = ((LogicalUnion) setOp).getConstantExprsList();
            if (!constExprsList.isEmpty()) {
                List<List<TOperator>> constRows = new ArrayList<>();
                for (List<NamedExpression> row : constExprsList) {
                    constRows.add(scalarTranslator.translateList(row));
                }
                tsetop.setChildren_const_expr_list(constRows);
            }
        }
        TOperatorUnion opUnion = new TOperatorUnion();
        opUnion.setLogical_set_op(tsetop);
        emit(setOp, opType, opUnion, texprs);
    }

    /**
     * Doris LogicalRepeat → horn TLogicalGrouping。NormalizeRepeat 已 desugar：groupingSets expr
     * 重写为 nullable、GROUPING_ID 已填入 repeat.getGroupingId()、Grouping/GroupingId 函数上提外层 Agg。
     * 共用列重映射（col_to_grouping_col_map）由 cc 端 uid 反查源表达式自行派生，FE 不去重。
     */
    @Override
    public Void visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, List<TExpression> texprs) {
        // 1. grouping_set_list
        List<List<TOperator>> tGroupingSets = new ArrayList<>(repeat.getGroupingSets().size());
        for (List<Expression> set : repeat.getGroupingSets()) {
            tGroupingSets.add(scalarTranslator.translateList(set));
        }

        // 2. grouping_expr_list = flatten distinct
        Set<Expression> groupingExprSet = new LinkedHashSet<>();
        for (List<Expression> set : repeat.getGroupingSets()) {
            groupingExprSet.addAll(set);
        }
        List<TOperator> tGroupingExprs = new ArrayList<>(groupingExprSet.size());
        for (Expression e : groupingExprSet) {
            tGroupingExprs.add(scalarTranslator.translate(e));
        }

        // 3. grouping_id_func：NormalizeRepeat 已无条件注入 GROUPING_ID 虚拟列，故 groupingId 恒 present。
        SlotReference groupingIdSlot = repeat.getGroupingId().get();
        TOperator tGroupingIdFunc = scalarTranslator.visitSlotReference(groupingIdSlot, null);

        // agg_fun_input_cols：repeat.outputExpressions 里「裸 SlotReference 且非 grouping key」的列，
        // 即上层 Agg 用到的 child 列（instanceof SlotReference && !groupingExprSet.contains）。
        // 顺带标记有无 GROUPING_PREFIX 生产者（GroupingScalarFunction），下面据此决定是否套 Project。
        boolean hasGroupingPrefix = false;
        List<TOperator> tAggFunInputCols = new ArrayList<>();
        for (NamedExpression out : repeat.getOutputExpressions()) {
            Expression inner = Doris2HornUtils.unwrapExpr(out);
            if (inner instanceof SlotReference && !groupingExprSet.contains(inner)) {
                tAggFunInputCols.add(scalarTranslator.visitSlotReference((SlotReference) inner, null));
            } else if (inner instanceof GroupingScalarFunction) {
                hasGroupingPrefix = true;
            }
        }

        TLogicalGrouping tlg = new TLogicalGrouping();
        tlg.setGrouping_set_list(tGroupingSets);
        tlg.setGrouping_expr_list(tGroupingExprs);
        tlg.setGrouping_id_func(Lists.newArrayList(tGroupingIdFunc));
        if (!tAggFunInputCols.isEmpty()) {
            tlg.setAgg_fun_input_cols(tAggFunInputCols);
        }
        TOperatorUnion opUnion = new TOperatorUnion();
        opUnion.setLogical_grouping(tlg);

        // GROUPING() 函数支持：grouping 上套普通 Project 透传原始 Grouping 函数（project_list 装
        // outputExpressions + GROUPING_ID，rewrite_list 是输出 slot），forward 注册 PREFIX slot 消 fallback，
        // backward 再还原进 Repeat。无 GROUPING() → 直接 emit grouping。
        if (!hasGroupingPrefix) {
            emit(repeat, TOperatorType.kLogicalGrouping, opUnion, texprs);
            return null;
        }
        List<NamedExpression> projects = new ArrayList<>(repeat.getOutputExpressions());
        projects.add(groupingIdSlot);
        List<TOperator> projectOutput = new ArrayList<>();
        for (NamedExpression project : projects) {
            projectOutput.add(scalarTranslator.visitSlotReference((SlotReference) project.toSlot(), null));
        }
        TLogicalProject projectOp = new TLogicalProject();
        projectOp.setProject_list(scalarTranslator.translateList(projects));
        projectOp.setRewrite_list(projectOutput);
        TOperatorUnion projectUnion = new TOperatorUnion();
        projectUnion.setLogical_project(projectOp);

        // pre-order：先 add Project texpr（arity=1 不递归），再 emit grouping（自带递归 child），
        // 形成 Project → grouping → child。
        TExpression projectTexpr = new TExpression();
        TOperator projectTop = new TOperator();
        projectTop.setOp_type(TOperatorType.kLogicalProject);
        projectTop.setOp_union(projectUnion);
        projectTexpr.setOp(projectTop);
        projectTexpr.setArity(1);
        projectTexpr.setOutput_list(projectOutput);
        texprs.add(projectTexpr);
        emit(repeat, TOperatorType.kLogicalGrouping, opUnion, texprs);
        return null;
    }

    /** agg 的 outputExpressions 中是否含带 distinct 标志的 AggregateFunction。 */
    private boolean hasDistinctAggFn(LogicalAggregate<? extends Plan> agg) {
        for (NamedExpression out : agg.getOutputExpressions()) {
            Expression inner = Doris2HornUtils.unwrapExpr(out);
            if (inner instanceof AggregateFunction && ((AggregateFunction) inner).isDistinct()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Doris LogicalWindow → horn TLogicalWindow（模板对齐 visitLogicalAggregate）。
     * 每个 WindowExpression 的 getFunction() → analytic_functions，对应输出 slot → analytic_computed_cols
     * （BE 按位对齐注册 slot_map）。partition_exprs / order_by_exprs / window_frame 取 leader spec。
     * 多 spec 由下面按等价类拆成单 spec window 链逐个翻译。
     */
    @Override
    public Void visitLogicalWindow(LogicalWindow<? extends Plan> logicalWindow, List<TExpression> texprs) {
        List<NamedExpression> windowExpressions = logicalWindow.getWindowExpressions();
        if (windowExpressions.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Horn: LogicalWindow with empty windowExpressions");
        }

        // 多 spec 拆分：按 (partition, order, frame) 等价类分组（复用 createWindowFrameGroups），
        // 多组则重写成单 spec LogicalWindow 链、逐个走单 spec 翻译。
        List<WindowFrameGroup> sameSpecGroups =
                LogicalWindowToPhysicalWindow.createWindowFrameGroups(windowExpressions);
        if (sameSpecGroups.size() > 1) {
            Plan windowChain = logicalWindow.child();
            for (WindowFrameGroup sameSpecGroup : sameSpecGroups) {
                windowChain = new LogicalWindow<>(sameSpecGroup.getGroups(), windowChain);
            }
            windowChain.accept(this, texprs);
            return null;
        }

        // 到这里 windowExpressions 必为同一 spec，partition/order/frame 取第一项套用整个 TLogicalWindow，
        // 逐项收集函数体 + 输出列。窗口列恒为 Alias(WindowExpression)。
        WindowExpression leaderWindow =
                (WindowExpression) Doris2HornUtils.unwrapExpr(windowExpressions.get(0));
        List<Expression> partitionKeys = leaderWindow.getPartitionKeys();
        List<OrderExpression> orderKeys = leaderWindow.getOrderKeys();
        Optional<WindowFrame> windowFrame = leaderWindow.getWindowFrame();

        List<TOperator> analyticFunctions = new ArrayList<>(windowExpressions.size());
        List<TOperator> analyticComputedColumns = new ArrayList<>(windowExpressions.size());
        for (NamedExpression windowExpression : windowExpressions) {
            WindowExpression analyticFunction =
                    (WindowExpression) Doris2HornUtils.unwrapExpr(windowExpression);
            analyticFunctions.add(scalarTranslator.translate(analyticFunction.getFunction()));
            // Alias.toSlot() 携带 ExprId + name + type —— 跟 agg_computed_cols 同样路径。
            analyticComputedColumns.add(scalarTranslator.visitSlotReference(
                    (SlotReference) windowExpression.toSlot(), null));
        }

        TLogicalWindow logicalWindowThrift = new TLogicalWindow();
        logicalWindowThrift.setAnalytic_functions(analyticFunctions);
        logicalWindowThrift.setAnalytic_computed_cols(analyticComputedColumns);
        if (!partitionKeys.isEmpty()) {
            logicalWindowThrift.setPartition_exprs(scalarTranslator.translateList(partitionKeys));
        }
        if (!orderKeys.isEmpty()) {
            List<OrderKey> windowOrderKeys = new ArrayList<>(orderKeys.size());
            for (OrderExpression orderExpr : orderKeys) {
                windowOrderKeys.add(orderExpr.getOrderKey());
            }
            logicalWindowThrift.setOrder_by_exprs(buildOrderSpec(windowOrderKeys));
        }
        if (windowFrame.isPresent()) {
            // 直接调 visitWindowFrame 得到 TOperator(kWindowFrame)，不走 translate()
            // （它会把 frame 包成 fscalar，BE 期望直接的 TScalar）。
            logicalWindowThrift.setWindow_frame(Collections.singletonList(
                    scalarTranslator.visitWindowFrame(windowFrame.get(), null)));
        }

        TOperatorUnion operatorUnion = new TOperatorUnion();
        operatorUnion.setLogical_window(logicalWindowThrift);
        emit(logicalWindow, TOperatorType.kLogicalWindow, operatorUnion, texprs);
        return null;
    }

    // sort / limit / topn 全 emit 成 TLogicalLimit（order_spec / limit / offset）：
    // 纯 sort → limit=-1,offset=0,order_spec=keys；纯 limit → limit/offset,order_spec=null；
    // topn → limit/offset + order_spec=keys。

    @Override
    public Void visitLogicalSort(LogicalSort<? extends Plan> sort, List<TExpression> texprs) {
        // sort wrapper：limit=-1 表示纯 ORDER BY，没有 split 概念
        buildLimit(sort, -1L, 0L, sort.getOrderKeys(), true, false, texprs);
        return null;
    }

    @Override
    public Void visitLogicalLimit(LogicalLimit<? extends Plan> limit, List<TExpression> texprs) {
        // phase != ORIGIN 表明已被 Nereids 拆出 Local/Global → is_split=true，BE 不再二次 split；
        // phase=LOCAL 时 is_global_limit=false，否则 true。
        boolean isSplit = limit.getPhase() != LimitPhase.ORIGIN;
        boolean isGlobal = limit.getPhase() != LimitPhase.LOCAL;
        buildLimit(limit, limit.getLimit(), limit.getOffset(), null, isGlobal, isSplit, texprs);
        return null;
    }

    @Override
    public Void visitLogicalTopN(LogicalTopN<? extends Plan> topn, List<TExpression> texprs) {
        // Doris LogicalTopN 没有 phase 字段 — 单一形态，相当于 ORIGIN，is_split=false
        buildLimit(topn, topn.getLimit(), topn.getOffset(), topn.getOrderKeys(), true, false, texprs);
        return null;
    }

    /** 公共 emit helper：visitLogicalSort/Limit/TopN 三个变体共用同一份 TLogicalLimit wire。 */
    private void buildLimit(Plan plan, long limit, long offset,
                            List<OrderKey> orderKeys, boolean isGlobal, boolean isSplit,
                            List<TExpression> texprs) {
        TLogicalLimit tlimit = new TLogicalLimit();
        tlimit.setLimit(limit);
        tlimit.setOffset(offset);
        tlimit.setIs_global_limit(isGlobal);
        tlimit.setIs_split(isSplit);
        if (orderKeys != null && !orderKeys.isEmpty()) {
            tlimit.setOrder_spec(buildOrderSpec(orderKeys));
        }
        TOperatorUnion opUnion = new TOperatorUnion();
        opUnion.setLogical_limit(tlimit);
        emit(plan, TOperatorType.kLogicalLimit, opUnion, texprs);
    }

    /** 在 scan 输出里按源列名（忽略大小写）找对应 SlotReference，找不到返回 null；供
     * base_table_order_spec 与 distribution_spec 把 key/分桶列对应到 scan 输出 slot。
     * 按名字匹配对齐 mainline（列名比 length/type 稳定），匹配 getOriginalColumn().getName()。 */
    private SlotReference findOutputSlotByColumn(List<Slot> outputSlots, String columnName) {
        for (Slot slot : outputSlots) {
            if (slot instanceof SlotReference) {
                Optional<Column> originalColumn = ((SlotReference) slot).getOriginalColumn();
                if (originalColumn.isPresent()
                        && originalColumn.get().getName().equalsIgnoreCase(columnName)) {
                    return (SlotReference) slot;
                }
            }
        }
        return null;
    }

    /** 把 Doris OrderKey 列表翻成 horn TOrderSpec (parallel lists 形态)。 */
    private TOrderSpec buildOrderSpec(List<OrderKey> orderKeys) {
        TOrderSpec orderSpec = new TOrderSpec();
        List<TOperator> exprList = new ArrayList<>(orderKeys.size());
        List<Boolean> ascList = new ArrayList<>(orderKeys.size());
        List<Boolean> nullsFirstList = new ArrayList<>(orderKeys.size());
        for (OrderKey orderKey : orderKeys) {
            exprList.add(scalarTranslator.translate(orderKey.getExpr()));
            ascList.add(orderKey.isAsc());
            nullsFirstList.add(orderKey.isNullFirst());
        }
        orderSpec.setOrder_by_exprs(exprList);
        orderSpec.setIs_asc_order(ascList);
        orderSpec.setNulls_first(nullsFirstList);
        return orderSpec;
    }

    @Override
    public Void visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, List<TExpression> texprs) {
        // 全部 10 种 Doris JoinType 映射到对应 horn logical join op_type。
        // Mark join（IN / EXISTS-OR 改写成 LEFT_SEMI mark join）：horn 不重排 semi，round-trip 即可；
        // 唯独 NULL_AWARE_LEFT_ANTI（NOT IN）的 null-aware mark 语义 horn 不实现 → fallback。
        TOperatorType opType;
        switch (join.getJoinType()) {
            case INNER_JOIN:
                opType = TOperatorType.kLogicalInnerJoin;
                break;
            case CROSS_JOIN:
                opType = TOperatorType.kLogicalCrossJoin;
                break;
            case LEFT_OUTER_JOIN:
                opType = TOperatorType.kLogicalLeftOuterJoin;
                break;
            case RIGHT_OUTER_JOIN:
                opType = TOperatorType.kLogicalRightOuterJoin;
                break;
            case FULL_OUTER_JOIN:
                opType = TOperatorType.kLogicalFullOuterJoin;
                break;
            case LEFT_SEMI_JOIN:
                opType = TOperatorType.kLogicalLeftSemiJoin;
                break;
            case RIGHT_SEMI_JOIN:
                opType = TOperatorType.kLogicalRightSemiJoin;
                break;
            case LEFT_ANTI_JOIN:
                opType = TOperatorType.kLogicalLeftAntiSemiJoin;
                break;
            case RIGHT_ANTI_JOIN:
                opType = TOperatorType.kLogicalRightAntiSemiJoin;
                break;
            case NULL_AWARE_LEFT_ANTI_JOIN:
                // null-aware mark join（NOT IN）的 null 传播语义 horn 不实现 → fallback Nereids。
                if (join.isMarkJoin()) {
                    throw new UnsupportedOperationException(
                            "Horn: null-aware mark join (NOT IN) not supported");
                }
                opType = TOperatorType.kLogicalNullAwareLeftAntiSemiJoin;
                break;
            default:
                throw new UnsupportedOperationException(
                        "Horn: unsupported join type: " + join.getJoinType());
        }
        TOperatorUnion opUnion = new TOperatorUnion();
        TLogicalJoin tjoin = new TLogicalJoin();
        if (!join.getHashJoinConjuncts().isEmpty()) {
            // 等值 join 条件 (hash join 用)
            tjoin.setEqual_join_predicate_list(
                    scalarTranslator.translateList(new ArrayList<>(join.getHashJoinConjuncts())));
        }
        if (!join.getOtherJoinConjuncts().isEmpty()) {
            // 非等值 / 后置 join 条件 (post-match filter)
            tjoin.setOther_join_predicate_list(
                    scalarTranslator.translateList(new ArrayList<>(join.getOtherJoinConjuncts())));
        }
        // mark join：emit mark_slot（MarkJoinSlotReference 携带 external_slot_id=ExprId）。
        // markJoinConjuncts 非空时一并 emit；cc 端注册 uid 供上层 Filter 引用 $c$N 时 Resolve。
        if (join.isMarkJoin()) {
            // mark_slot 用单元素 list（同 grouping_id_func）规避 thrift TOperator 递归。
            tjoin.setMark_slot(Lists.newArrayList(scalarTranslator.visitSlotReference(
                    join.getMarkJoinSlotReference().get(), null)));
            if (!join.getMarkJoinConjuncts().isEmpty()) {
                tjoin.setMark_join_predicate_list(
                        scalarTranslator.translateList(new ArrayList<>(join.getMarkJoinConjuncts())));
            }
        }
        opUnion.setLogical_join(tjoin);
        // emit 内 plan.children() = [left, right] 顺序 (BinaryPlan)
        emit(join, opType, opUnion, texprs);
        return null;
    }
}
