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

package org.apache.doris.nereids.processor.post.runtimeFilterV2;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.thrift.TRuntimeFilterType;

public class RuntimeFilterV2 {
    private RuntimeFilterId id;
    private AbstractPhysicalPlan sourceNode;
    private Expression sourceExpression;
    private AbstractPhysicalPlan targetNode;
    private Expression targetExpression;
    private TRuntimeFilterType type;

    public RuntimeFilterV2(RuntimeFilterId id, AbstractPhysicalPlan sourceNode, Expression source,
            AbstractPhysicalPlan targetNode, Expression target, TRuntimeFilterType type) {
        this.sourceNode = sourceNode;
        this.sourceExpression = source;
        this.targetNode = targetNode;
        this.targetExpression = target;
        this.id = id;
        this.type = type;
    }

    public RuntimeFilterId getId() {
        return id;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RF").append(id.asInt()).append("[").append(type).append("]")
                .append("(").append(sourceExpression)
                .append("->").append(targetExpression).append(")");
        return sb.toString();
    }

}
