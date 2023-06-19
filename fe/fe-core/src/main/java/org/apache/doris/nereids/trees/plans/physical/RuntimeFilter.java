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
import org.apache.doris.thrift.TRuntimeFilterType;

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
    private final List<Expression> targetExpressions;
    private final List<Slot> targetSlots;
    private final int exprOrder;
    private final AbstractPhysicalJoin builderNode;

    private final boolean bitmapFilterNotIn;

    private final long buildSideNdv;

    /**
     * constructor
     */
    public RuntimeFilter(RuntimeFilterId id, Expression src, List<Slot> targets, TRuntimeFilterType type,
            int exprOrder, AbstractPhysicalJoin builderNode, long buildSideNdv) {
        this(id, src, targets, ImmutableList.copyOf(targets), type, exprOrder, builderNode, false, buildSideNdv);
    }

    /**
     * constructor
     */
    public RuntimeFilter(RuntimeFilterId id, Expression src, List<Slot> targets, List<Expression> targetExpressions,
            TRuntimeFilterType type, int exprOrder, AbstractPhysicalJoin builderNode, boolean bitmapFilterNotIn,
            long buildSideNdv) {
        this.id = id;
        this.srcSlot = src;
        this.targetSlots = ImmutableList.copyOf(targets);
        this.targetExpressions = ImmutableList.copyOf(targetExpressions);
        this.type = type;
        this.exprOrder = exprOrder;
        this.builderNode = builderNode;
        this.bitmapFilterNotIn = bitmapFilterNotIn;
        this.buildSideNdv = buildSideNdv <= 0 ? -1L : buildSideNdv;
        builderNode.addRuntimeFilter(this);
    }

    public Expression getSrcExpr() {
        return srcSlot;
    }

    public List<Slot> getTargetExprs() {
        return targetSlots;
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

    public AbstractPhysicalJoin getBuilderNode() {
        return builderNode;
    }

    public boolean isBitmapFilterNotIn() {
        return bitmapFilterNotIn;
    }

    public List<Expression> getTargetExpressions() {
        return targetExpressions;
    }

    public long getBuildSideNdv() {
        return buildSideNdv;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RF").append(id.asInt())
                .append("[").append(getSrcExpr()).append("->").append(targetSlots)
                .append("(ndv/size = ").append(buildSideNdv).append("/")
                .append(org.apache.doris.planner.RuntimeFilter.expectRuntimeFilterSize(buildSideNdv))
                .append(")");
        return sb.toString();
    }
}
