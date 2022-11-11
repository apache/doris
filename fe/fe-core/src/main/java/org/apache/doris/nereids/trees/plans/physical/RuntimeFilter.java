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

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.thrift.TRuntimeFilterType;

/**
 * runtime filter
 */
public class RuntimeFilter {

    private final Slot srcSlot;

    private Slot targetSlot;

    private final RuntimeFilterId id;

    private final TRuntimeFilterType type;

    private final int exprOrder;

    private boolean finalized = false;

    private PhysicalHashJoin builderNode;

    /**
     * constructor
     */
    public RuntimeFilter(RuntimeFilterId id, Slot src, Slot target, TRuntimeFilterType type,
            int exprOrder, PhysicalHashJoin builderNode) {
        this.id = id;
        this.srcSlot = src;
        this.targetSlot = target;
        this.type = type;
        this.exprOrder = exprOrder;
        this.builderNode = builderNode;
    }

    public Slot getSrcExpr() {
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

    public PhysicalHashJoin getBuilderNode() {
        return builderNode;
    }

    public void setFinalized() {
        this.finalized = true;
    }
}
