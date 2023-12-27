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

package org.apache.doris.nereids.jobs.joinorder.hypergraph.edge;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import java.util.BitSet;
import java.util.List;
import java.util.Set;

/**
 * Edge represents a filter
 */
public class FilterEdge extends Edge {
    private final LogicalFilter<? extends Plan> filter;

    public FilterEdge(LogicalFilter<? extends Plan> filter, int index,
            BitSet childEdges, long subTreeNodes, long childRequireNodes) {
        super(index, childEdges, new BitSet(), subTreeNodes, childRequireNodes, 0L);
        this.filter = filter;
    }

    @Override
    public Set<Slot> getInputSlots() {
        return filter.getInputSlots();
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return filter.getExpressions();
    }
}
