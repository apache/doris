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

import org.apache.doris.analysis.Expr;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.thrift.TTopnFilterDesc;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * topn filter
 */
public class TopnFilter {
    public TopN topn;
    public SortNode legacySortNode;
    public Map<PhysicalRelation, Expression> targets = Maps.newHashMap();
    public Map<ScanNode, Expr> legacyTargets = Maps.newHashMap();

    public TopnFilter(TopN topn, PhysicalRelation rel, Expression expr) {
        this.topn = topn;
        targets.put(rel, expr);
    }

    public void addTarget(PhysicalRelation rel, Expression expr) {
        targets.put(rel, expr);
    }

    public boolean hasTargetRelation(PhysicalRelation rel) {
        return targets.containsKey(rel);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(topn).append("->[ ");
        for (PhysicalRelation rel : targets.keySet()) {
            builder.append("(").append(rel).append(":").append(targets.get(rel)).append(") ");
        }
        builder.append("]");
        return builder.toString();
    }

    /**
     * to thrift
     */
    public TTopnFilterDesc toThrift() {
        TTopnFilterDesc tFilter = new TTopnFilterDesc();
        tFilter.setSourceNodeId(legacySortNode.getId().asInt());
        tFilter.setIsAsc(topn.getOrderKeys().get(0).isAsc());
        tFilter.setNullFirst(topn.getOrderKeys().get(0).isNullFirst());
        for (ScanNode scan : legacyTargets.keySet()) {
            tFilter.putToTargetNodeIdToTargetExpr(scan.getId().asInt(),
                    legacyTargets.get(scan).treeToThrift());
        }
        return tFilter;
    }
}
