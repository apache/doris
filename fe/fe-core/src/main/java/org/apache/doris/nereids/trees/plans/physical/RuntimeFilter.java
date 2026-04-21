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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * runtime filter
 */
public class RuntimeFilter {

    private final RuntimeFilterId id;
    private final TRuntimeFilterType type;
    private final Expression srcSlot;
    //bitmap filter support target expression like  k1+1, abs(k1)
    //targetExpression is an expression on targetSlot, in which there is only one non-const slot
    private final Expression targetExpression;
    private final Slot targetSlot;
    private final int exprOrder;
    private final AbstractPhysicalPlan builderNode;

    private final boolean bitmapFilterNotIn;

    private final long buildSideNdv;
    // use for min-max filter only. specify if the min or max side is valid
    private final TMinMaxRuntimeFilterType tMinMaxType;

    private final PhysicalRelation targetScan;

    private final boolean bloomFilterSizeCalculatedByNdv;

    /**
     * constructor
     */
    public RuntimeFilter(RuntimeFilterId id, Expression src, Slot targetSlot, Expression targetExpression,
                         TRuntimeFilterType type, int exprOrder, AbstractPhysicalPlan builderNode, long buildSideNdv,
                           boolean bloomFilterSizeCalculatedByNdv, TMinMaxRuntimeFilterType tMinMaxType,
                           PhysicalRelation scan) {
        this(id, src, targetSlot, targetExpression, type, exprOrder,
                builderNode, false, buildSideNdv, bloomFilterSizeCalculatedByNdv,
                tMinMaxType, scan);
    }

    public RuntimeFilter(RuntimeFilterId id, Expression src, Slot targetSlot, Expression targetExpression,
                         TRuntimeFilterType type, int exprOrder, AbstractPhysicalPlan builderNode,
                         boolean bitmapFilterNotIn, long buildSideNdv, boolean bloomFilterSizeCalculatedByNdv,
                         PhysicalRelation scan) {
        this(id, src, targetSlot, targetExpression, type, exprOrder,
                builderNode, bitmapFilterNotIn, buildSideNdv, bloomFilterSizeCalculatedByNdv,
                TMinMaxRuntimeFilterType.MIN_MAX, scan);
    }

    /**
     * constructor
     */
    public RuntimeFilter(RuntimeFilterId id, Expression src, Slot targetSlot, Expression targetExpression,
                         TRuntimeFilterType type, int exprOrder, AbstractPhysicalPlan builderNode,
                         boolean bitmapFilterNotIn, long buildSideNdv, boolean bloomFilterSizeCalculatedByNdv,
                         TMinMaxRuntimeFilterType tMinMaxType,
                         PhysicalRelation scan) {
        this.id = id;
        this.srcSlot = src;
        this.targetSlot = targetSlot;
        this.targetExpression = targetExpression;
        this.type = type;
        this.exprOrder = exprOrder;
        this.builderNode = builderNode;
        this.bitmapFilterNotIn = bitmapFilterNotIn;
        this.bloomFilterSizeCalculatedByNdv = bloomFilterSizeCalculatedByNdv;
        this.buildSideNdv = buildSideNdv <= 0 ? -1L : buildSideNdv;
        this.tMinMaxType = tMinMaxType;
        builderNode.addRuntimeFilter(this);
        this.targetScan = scan;
    }

    // Keep old list-based overloads for binary compatibility with stale incremental-build classes.
    public RuntimeFilter(RuntimeFilterId id, Expression src, List<Slot> targets, List<Expression> targetExpressions,
                         TRuntimeFilterType type, int exprOrder, AbstractPhysicalPlan builderNode, long buildSideNdv,
                         boolean bloomFilterSizeCalculatedByNdv, TMinMaxRuntimeFilterType tMinMaxType,
                         PhysicalRelation scan) {
        this(id, src, extractSingleTargetSlot(targets), extractSingleTargetExpression(targetExpressions),
                type, exprOrder, builderNode, buildSideNdv, bloomFilterSizeCalculatedByNdv, tMinMaxType, scan);
    }

    public RuntimeFilter(RuntimeFilterId id, Expression src, List<Slot> targets, List<Expression> targetExpressions,
                         TRuntimeFilterType type, int exprOrder, AbstractPhysicalPlan builderNode,
                         boolean bitmapFilterNotIn, long buildSideNdv, boolean bloomFilterSizeCalculatedByNdv,
                         PhysicalRelation scan) {
        this(id, src, extractSingleTargetSlot(targets), extractSingleTargetExpression(targetExpressions),
                type, exprOrder, builderNode, bitmapFilterNotIn, buildSideNdv, bloomFilterSizeCalculatedByNdv,
                scan);
    }

    public RuntimeFilter(RuntimeFilterId id, Expression src, List<Slot> targets, List<Expression> targetExpressions,
                         TRuntimeFilterType type, int exprOrder, AbstractPhysicalPlan builderNode,
                         boolean bitmapFilterNotIn, long buildSideNdv, boolean bloomFilterSizeCalculatedByNdv,
                         TMinMaxRuntimeFilterType tMinMaxType, PhysicalRelation scan) {
        this(id, src, extractSingleTargetSlot(targets), extractSingleTargetExpression(targetExpressions),
                type, exprOrder, builderNode, bitmapFilterNotIn, buildSideNdv, bloomFilterSizeCalculatedByNdv,
                tMinMaxType, scan);
    }

    private static Slot extractSingleTargetSlot(List<Slot> targets) {
        Preconditions.checkArgument(targets.size() == 1, "runtime filter expects exactly one target slot");
        return targets.get(0);
    }

    private static Expression extractSingleTargetExpression(List<Expression> targetExpressions) {
        Preconditions.checkArgument(targetExpressions.size() == 1,
                "runtime filter expects exactly one target expression");
        return targetExpressions.get(0);
    }

    public TMinMaxRuntimeFilterType gettMinMaxType() {
        return tMinMaxType;
    }

    public Expression getSrcExpr() {
        return srcSlot;
    }

    public RuntimeFilterId getId() {
        return id;
    }

    public TRuntimeFilterType getType() {
        return type;
    }

    public int getExprOrder() {
        return exprOrder;
    }

    public AbstractPhysicalPlan getBuilderNode() {
        return builderNode;
    }

    public boolean isBitmapFilterNotIn() {
        return bitmapFilterNotIn;
    }

    public Expression getTargetExpression() {
        return targetExpression;
    }

    public long getBuildSideNdv() {
        return buildSideNdv;
    }

    public Slot getTargetSlot() {
        return targetSlot;
    }

    public List<Slot> getTargetSlots() {
        return ImmutableList.of(targetSlot);
    }

    public PhysicalRelation getTargetScan() {
        return targetScan;
    }

    public List<PhysicalRelation> getTargetScans() {
        return ImmutableList.of(targetScan);
    }

    public List<Expression> getTargetExpressions() {
        return ImmutableList.of(targetExpression);
    }

    public boolean hasTargetScan(PhysicalRelation scan) {
        return targetScan.equals(scan);
    }

    @Override
    public String toString() {
        String ignore = "";
        if (ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable()
                .getIgnoredRuntimeFilterIds().contains(id.asInt())) {
            ignore = "(ignored)";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(ignore).append("RF").append(id.asInt()).append(" ")
                .append(getSrcExpr()).append("->").append(targetExpression)
                .append("(ndv/size = ").append(buildSideNdv).append("/")
                .append(org.apache.doris.planner.RuntimeFilter.expectRuntimeFilterSize(buildSideNdv))
                .append(")");
        return sb.toString();
    }

    /**
     * print rf in explain shape plan
     * @return brief version of toString()
     */
    public String shapeInfo() {
        String ignore = "";
        if (ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable()
                .getIgnoredRuntimeFilterIds().contains(id.asInt())) {
            ignore = "(ignored)";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(ignore).append("RF").append(id.asInt())
                .append(" ").append(getSrcExpr().toSql()).append("->").append(targetExpression.toSql());
        return sb.toString();
    }

    public boolean isBloomFilterSizeCalculatedByNdv() {
        return bloomFilterSizeCalculatedByNdv;
    }
}
