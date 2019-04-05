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

package org.apache.doris.optimizer.base;

import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.stat.Statistics;

import java.util.List;

// Context used to compute cost for an MultiExpression
public class OptCostContext {
    private MultiExpression multiExpr;
    private OptimizationContext optCtx;
    private OptCost cost;
    private Statistics statistics;
    private List<OptimizationContext> childrenCtxs;

    private OptPhysicalProperty derivedPhysicalProperty;

    public OptCostContext(MultiExpression multiExpr, OptimizationContext optCtx) {
        this.multiExpr = multiExpr;
        this.optCtx = optCtx;
    }

    public MultiExpression getMultiExpr() { return multiExpr; }
    public OptCost getCost() { return cost; }
    public Statistics getStatistics() { return statistics; }
    public void setChildrenCtxs(List<OptimizationContext> childrenCtxs) { this.childrenCtxs = childrenCtxs; }
    public OptPhysicalProperty getDerivedPhysicalProperty() { return derivedPhysicalProperty; }
    public OptimizationContext getInput(int idx) { return childrenCtxs.get(idx); }
}
