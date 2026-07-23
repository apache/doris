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


import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.nereids.properties.ChildOutputPropertyDeriver;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecStorageAny;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsCache;

import org.apache.horn4j.thrift.TExpression;
import org.apache.horn4j.thrift.TOperator;
import org.apache.horn4j.thrift.TOperatorType;
import org.apache.horn4j.thrift.TOperatorUnion;
import org.apache.horn4j.thrift.TScalar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * horn2doris 反译路径下的纯静态辅助方法集中点 —— 跟 {@link DorisPhysicalPlanBuilder}
 * 实例状态无关的逻辑都放这里。
 */
public final class Horn2DorisUtils {

    private static final Logger LOG = LogManager.getLogger(Horn2DorisUtils.class);

    private Horn2DorisUtils() {
    }

    /**
     * 从 TOperator 取 root TScalar，兼容 scalar 单节点 / fscalar 扁平树（root = scalars[0]）两种形态。
     */
    public static TScalar getRootScalar(TOperator op) {
        TOperatorUnion union = op.getOp_union();
        if (union.isSetFscalar()) {
            List<TOperator> scalars = union.getFscalar().getScalars();
            if (scalars == null || scalars.isEmpty()) {
                throw new IllegalArgumentException(
                        "Horn: fscalar with empty scalars list, op_type=" + op.getOp_type());
            }
            return scalars.get(0).getOp_union().getScalar();
        }
        if (!union.isSetScalar()) {
            throw new IllegalArgumentException(
                    "Horn: TOperator has neither scalar nor fscalar payload, op_type="
                            + op.getOp_type());
        }
        return union.getScalar();
    }

    public static boolean isHashJoin(TOperatorType opType) {
        switch (opType) {
            case kPhysicalInnerHashJoin:
            case kPhysicalLeftOuterHashJoin:
            case kPhysicalRightOuterHashJoin:
            case kPhysicalFullOuterHashJoin:
            case kPhysicalLeftSemiHashJoin:
            case kPhysicalRightSemiHashJoin:
            case kPhysicalLeftAntiSemiHashJoin:
            case kPhysicalRightAntiSemiHashJoin:
            case kPhysicalNullAwareLeftAntiSemiHashJoin:
                return true;
            default:
                return false;
        }
    }


    public static JoinType getJoinType(TOperatorType opType) {
        switch (opType) {
            case kPhysicalInnerHashJoin:
            case kPhysicalInnerNestedLoopJoin:
                return JoinType.INNER_JOIN;
            case kPhysicalLeftOuterHashJoin:
            case kPhysicalLeftOuterNestedLoopJoin:
                return JoinType.LEFT_OUTER_JOIN;
            case kPhysicalRightOuterHashJoin:
            case kPhysicalRightOuterNestedLoopJoin:
                return JoinType.RIGHT_OUTER_JOIN;
            case kPhysicalFullOuterHashJoin:
            case kPhysicalFullOuterNestedLoopJoin:
                return JoinType.FULL_OUTER_JOIN;
            case kPhysicalLeftSemiHashJoin:
            case kPhysicalLeftSemiNestedLoopJoin:
                return JoinType.LEFT_SEMI_JOIN;
            case kPhysicalRightSemiHashJoin:
            case kPhysicalRightSemiNestedLoopJoin:
                return JoinType.RIGHT_SEMI_JOIN;
            case kPhysicalLeftAntiSemiHashJoin:
            case kPhysicalLeftAntiSemiNestedLoopJoin:
                return JoinType.LEFT_ANTI_JOIN;
            case kPhysicalRightAntiSemiHashJoin:
            case kPhysicalRightAntiSemiNestedLoopJoin:
                return JoinType.RIGHT_ANTI_JOIN;
            case kPhysicalNullAwareLeftAntiSemiHashJoin:
            case kPhysicalNullAwareLeftAntiSemiNestedLoopJoin:
                return JoinType.NULL_AWARE_LEFT_ANTI_JOIN;
            case kPhysicalCrossJoin:
                return JoinType.CROSS_JOIN;
            default:
                throw new IllegalStateException("Horn: unsupported join op_type: " + opType);
        }
    }

    /**
     * 用 Horn TExpression.cost_info.row_numbers 作行数，列 stats 从 Doris StatisticsCache 查。
     * 仅 base-table 列（SlotReference 带 OlapTable + originalColumn）查真实 stats，其余回退 UNKNOWN。
     */
    public static Statistics buildStatistics(TExpression texpr, PhysicalPlan plan) {
        double rowCount = 1.0;
        if (texpr.isSetCost_info() && texpr.getCost_info().isSetRow_numbers()) {
            rowCount = Math.max(texpr.getCost_info().getRow_numbers(), 1.0);
        }
        Map<Expression, ColumnStatistic> colStats = new HashMap<>();
        ConnectContext ctx = ConnectContext.get();
        StatisticsCache statsCache = ctx != null ? Env.getCurrentEnv().getStatisticsCache() : null;
        for (Slot slot : plan.getOutput()) {
            colStats.put(slot, resolveColumnStatistic(slot, statsCache, ctx));
        }
        return new Statistics(rowCount, colStats);
    }

    private static ColumnStatistic resolveColumnStatistic(
            Slot slot, StatisticsCache statsCache, ConnectContext ctx) {
        if (statsCache == null || !(slot instanceof SlotReference)) {
            return ColumnStatistic.UNKNOWN;
        }
        SlotReference sr = (SlotReference) slot;
        if (!sr.getOriginalTable().isPresent() || !sr.getOriginalColumn().isPresent()) {
            return ColumnStatistic.UNKNOWN;
        }
        if (!(sr.getOriginalTable().get() instanceof OlapTable)) {
            return ColumnStatistic.UNKNOWN;
        }
        OlapTable table = (OlapTable) sr.getOriginalTable().get();
        ColumnStatistic stat = statsCache.getColumnStatistics(
                table.getDatabase().getCatalog().getId(),
                table.getDatabase().getId(),
                table.getId(),
                table.getBaseIndexId(),
                sr.getOriginalColumn().get().getName(),
                ctx);
        return stat != null ? stat : ColumnStatistic.UNKNOWN;
    }

    /**
     * 给 plan 设 PhysicalProperties + Statistics。分布派生复用主线 {@code ChildOutputPropertyDeriver}。
     * 仅 {@code PhysicalOlapScan} / {@code PhysicalDistribute} 因 deriver 需解引用反译路径没有的
     * context，改用节点自带的 DistributionSpec；其余算子委托 deriver（传 {@code null} context 安全）。
     */
    public static PhysicalPlan setPhysicalProperties(TExpression texpr, PhysicalPlan plan) {
        PhysicalProperties props;
        if (plan instanceof PhysicalOlapScan) {
            props = new PhysicalProperties(((PhysicalOlapScan) plan).getDistributionSpec());
        } else if (plan instanceof PhysicalDistribute) {
            props = new PhysicalProperties(((PhysicalDistribute<?>) plan).getDistributionSpec());
        } else {
            // union 孩子混合 NATURAL 与 STORAGE_BUCKETED 时无损重贴标签，避免 deriver 降级、保住 bucketShuffle
            if (plan instanceof PhysicalUnion) {
                plan = relabelUnionNaturalChildrenToStorageBucketed((PhysicalUnion) plan);
            }
            List<PhysicalProperties> childProps = new ArrayList<>();
            for (Plan child : plan.children()) {
                childProps.add(((PhysicalPlan) child).getPhysicalProperties());
            }
            props = plan.accept(new ChildOutputPropertyDeriver(childProps), null);
        }
        return (PhysicalPlan) plan.withPhysicalPropertiesAndStats(
                props, buildStatistics(texpr, plan));
    }

    /**
     * intersect/except 输出列 nullable 对齐——复刻 Doris {@code LogicalSetOperation.buildNewOutputs}
     * （output nullable = 各孩子对应列 nullable 的 OR）。BE {@code set_source_operator} 严格校验此关系，
     * 不一致即报错；仅 intersect/except 需要，union 的 BE 无此校验。
     */
    public static List<NamedExpression> alignSetOpOutputNullable(
            List<NamedExpression> outputs, List<List<SlotReference>> childrenOutputs) {
        List<NamedExpression> aligned = new ArrayList<>(outputs.size());
        for (int i = 0; i < outputs.size(); i++) {
            boolean childOrNullable = false;
            for (List<SlotReference> childCols : childrenOutputs) {
                if (i < childCols.size() && childCols.get(i).nullable()) {
                    childOrNullable = true;
                    break;
                }
            }
            NamedExpression out = outputs.get(i);
            if (out instanceof SlotReference && ((SlotReference) out).nullable() != childOrNullable) {
                aligned.add(((SlotReference) out).withNullable(childOrNullable));
            } else {
                aligned.add(out);
            }
        }
        return aligned;
    }

    /**
     * union 孩子若同时有 NATURAL 与 STORAGE_BUCKETED 的 DistributionSpecHash 且对齐同一张表存储布局
     * （tableId + indexId + partitionIds 全相同），把 NATURAL 无损重贴为 STORAGE_BUCKETED，使各孩子
     * shuffleType 字面一致，让 union 产出干净 Hash、上层 join 得以 bucketShuffle。copy-on-write，不改原节点。
     */
    private static PhysicalUnion relabelUnionNaturalChildrenToStorageBucketed(PhysicalUnion union) {
        long tableId = -1;
        long indexId = -1;
        java.util.Set<Long> partitionIds = null;
        boolean hasNatural = false;
        boolean hasStorageBucketed = false;
        boolean aligned = true;
        for (Plan child : union.children()) {
            DistributionSpec spec = ((PhysicalPlan) child).getPhysicalProperties().getDistributionSpec();
            if (!(spec instanceof DistributionSpecHash)) {
                return union;  // 有非 Hash 孩子 → 不适用
            }
            DistributionSpecHash hash = (DistributionSpecHash) spec;
            ShuffleType st = hash.getShuffleType();
            if (st == ShuffleType.NATURAL) {
                hasNatural = true;
            } else if (st == ShuffleType.STORAGE_BUCKETED) {
                hasStorageBucketed = true;
            } else {
                return union;  // 其它 shuffleType（EXECUTION_BUCKETED 等）→ 不动
            }
            if (tableId == -1) {
                tableId = hash.getTableId();
                indexId = hash.getSelectedIndexId();
                partitionIds = hash.getPartitionIds();
            } else if (tableId != hash.getTableId() || indexId != hash.getSelectedIndexId()
                    || !partitionIds.equals(hash.getPartitionIds())) {
                aligned = false;
            }
        }
        if (!hasNatural || !hasStorageBucketed || !aligned) {
            return union;  // 无混合，或跨表/桶布局不同 → 不重贴
        }
        List<Plan> newChildren = new ArrayList<>();
        for (Plan child : union.children()) {
            PhysicalPlan pc = (PhysicalPlan) child;
            DistributionSpecHash hash = (DistributionSpecHash) pc.getPhysicalProperties().getDistributionSpec();
            if (hash.getShuffleType() == ShuffleType.NATURAL) {
                PhysicalProperties relabeled = new PhysicalProperties(
                        hash.withShuffleType(ShuffleType.STORAGE_BUCKETED));
                newChildren.add((Plan) pc.withPhysicalPropertiesAndStats(relabeled, pc.getStats()));
            } else {
                newChildren.add(child);
            }
        }
        return union.withChildren(newChildren);
    }

    /**
     * 给 scan 算子构造 colocate-capable DistributionSpec —— 仿主线
     * {@code LogicalOlapScanToPhysicalOlapScan.convertDistribution}。HashDistributionInfo +
     * (stable colocate group || 单分区) → 带 tableId/indexId/partitionIds 的 NATURAL hash spec，
     * 否则 fallback {@code DistributionSpecStorageAny.INSTANCE}。
     */
    public static DistributionSpec buildScanDistributionSpec(
            OlapTable olapTable, List<Slot> scanSlots, List<Long> partitionIds) {
        DistributionInfo distInfo = olapTable.getDefaultDistributionInfo();
        ColocateTableIndex colocateIndex = Env.getCurrentColocateIndex();
        boolean isBelongStableCG = colocateIndex.isColocateTable(olapTable.getId())
                && !colocateIndex.isGroupUnstable(colocateIndex.getGroup(olapTable.getId()))
                && olapTable.getCatalogId() == Env.getCurrentInternalCatalog().getId();
        boolean isSelectUnpartition = olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED
                || partitionIds.size() == 1;
        if (!(distInfo instanceof HashDistributionInfo) || (!isBelongStableCG && !isSelectUnpartition)) {
            return DistributionSpecStorageAny.INSTANCE;
        }
        HashDistributionInfo hashDist = (HashDistributionInfo) distInfo;
        List<ExprId> hashExprIds = new ArrayList<>();
        for (Column distCol : hashDist.getDistributionColumns()) {
            for (Slot slot : scanSlots) {
                if (slot instanceof SlotReference
                        && slot.getName().equalsIgnoreCase(distCol.getName())) {
                    hashExprIds.add(slot.getExprId());
                    break;
                }
            }
        }
        return new DistributionSpecHash(hashExprIds, ShuffleType.NATURAL,
                olapTable.getId(), olapTable.getBaseIndexId(),
                new LinkedHashSet<>(partitionIds));
    }

}
