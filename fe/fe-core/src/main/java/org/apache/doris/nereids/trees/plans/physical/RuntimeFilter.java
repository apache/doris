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

/**
 * runtime filter
 */
public class RuntimeFilter {

    private final RuntimeFilterId id;
    private final TRuntimeFilterType type;
    private final Expression srcSlot;
    //bitmap filter support target expression like  k1+1, abs(k1)
    //targetExpression is an expression on targetSlot, in which there is only one non-const slot
    private Expression targetExpression;
    private Slot targetSlot;
    private final int exprOrder;
    private AbstractPhysicalJoin builderNode;

    private boolean bitmapFilterNotIn;

    /**
     * constructor
     */
    public RuntimeFilter(RuntimeFilterId id, Expression src, Slot target, TRuntimeFilterType type,
            int exprOrder, AbstractPhysicalJoin builderNode) {
        this(id, src, target, target, type, exprOrder, builderNode, false);
    }

    /**
     * constructor
     */
    public RuntimeFilter(RuntimeFilterId id, Expression src, Slot target, Expression targetExpression,
            TRuntimeFilterType type,
            int exprOrder, AbstractPhysicalJoin builderNode, boolean bitmapFilterNotIn) {
        this.id = id;
        this.srcSlot = src;
        this.targetSlot = target;
        this.targetExpression = targetExpression;
        this.type = type;
        this.exprOrder = exprOrder;
        this.builderNode = builderNode;
        this.bitmapFilterNotIn = bitmapFilterNotIn;
    }

    public Expression getSrcExpr() {
        return srcSlot;
    }

    public Slot getTargetExpr() {
        return targetSlot;
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

    public Expression getTargetExpression() {
        return targetExpression;
    }

}
