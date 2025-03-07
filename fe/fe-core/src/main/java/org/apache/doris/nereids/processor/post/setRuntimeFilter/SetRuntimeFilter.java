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

package org.apache.doris.nereids.processor.post.setRuntimeFilter;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.planner.RuntimeFilterId;

public class SetRuntimeFilter {
    private RuntimeFilterId id;
    private SetOperation node;
    private Expression source;
    private PhysicalCatalogRelation relation;
    private Expression target;

    public SetRuntimeFilter(RuntimeFilterId id, SetOperation node, Expression source,
            PhysicalCatalogRelation relation, Expression target) {
        this.node = node;
        this.source = source;
        this.relation = relation;
        this.target = target;
        this.id = id;
    }

    public RuntimeFilterId getId() {
        return id;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SRF").append(id.asInt()).append("(").append(source).append("->").append(target).append(")");
        return sb.toString();
    }

}
