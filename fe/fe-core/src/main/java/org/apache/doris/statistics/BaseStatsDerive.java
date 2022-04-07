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

package org.apache.doris.statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.analysis.Expr;
import org.apache.doris.planner.PlanNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class BaseStatsDerive {
    // estimate of the output cardinality of this node;
    // invalid: -1
    protected long cardinality;
    protected long limit;

    protected List<Expr> conjuncts = Lists.newArrayList();
    protected List<StatsDeriveResult> childrenStatsResult = Lists.newArrayList();

    protected BaseStatsDerive init(PlanNode node) {
        cardinality = -1;
        limit = node.getLimit();
        conjuncts.addAll(node.getConjuncts());

        for (PlanNode childNode : node.getChildren()) {
            childrenStatsResult.add(childNode.getStatsDeriveResult());
        }
        return this;
    }

    public abstract StatsDeriveResult deriveStats();

    public boolean hasLimit() {
        return limit > -1;
    }

    protected void applyConjunctsSelectivity() {
        if (cardinality == -1) {
            return;
        }
        applySelectivity();
    }

    private void applySelectivity() {
        double selectivity = computeSelectivity();
        Preconditions.checkState(cardinality >= 0);
        long preConjunctCardinality = cardinality;
        cardinality = Math.round(cardinality * selectivity);
        // don't round cardinality down to zero for safety.
        if (cardinality == 0 && preConjunctCardinality > 0) {
            cardinality = 1;
        }
    }

    protected double computeSelectivity() {
        for (Expr expr : conjuncts) {
            expr.setSelectivity();
        }
        return computeCombinedSelectivity(conjuncts);
    }

    /**
     * Returns the estimated combined selectivity of all conjuncts. Uses heuristics to
     * address the following estimation challenges:
     * 1. The individual selectivities of conjuncts may be unknown.
     * 2. Two selectivities, whether known or unknown, could be correlated. Assuming
     * independence can lead to significant underestimation.
     * <p>
     * The first issue is addressed by using a single default selectivity that is
     * representative of all conjuncts with unknown selectivities.
     * The second issue is addressed by an exponential backoff when multiplying each
     * additional selectivity into the final result.
     */
    static protected double computeCombinedSelectivity(List<Expr> conjuncts) {
        // Collect all estimated selectivities.
        List<Double> selectivities = new ArrayList<>();
        for (Expr e : conjuncts) {
            if (e.hasSelectivity()) selectivities.add(e.getSelectivity());
        }
        if (selectivities.size() != conjuncts.size()) {
            // Some conjuncts have no estimated selectivity. Use a single default
            // representative selectivity for all those conjuncts.
            selectivities.add(Expr.DEFAULT_SELECTIVITY);
        }
        // Sort the selectivities to get a consistent estimate, regardless of the original
        // conjunct order. Sort in ascending order such that the most selective conjunct
        // is fully applied.
        Collections.sort(selectivities);
        double result = 1.0;
        // selectivity = 1 * (s1)^(1/1) * (s2)^(1/2) * ... * (sn-1)^(1/(n-1)) * (sn)^(1/n)
        for (int i = 0; i < selectivities.size(); ++i) {
            // Exponential backoff for each selectivity multiplied into the final result.
            result *= Math.pow(selectivities.get(i), 1.0 / (double) (i + 1));
        }
        // Bound result in [0, 1]
        return Math.max(0.0, Math.min(1.0, result));
    }

    protected void capCardinalityAtLimit() {
        if (hasLimit()) {
            cardinality = cardinality == -1 ? limit : Math.min(cardinality, limit);
        }
    }
}
