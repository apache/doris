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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.analysis.Expr;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SortNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * topN runtime filter context
 */
public class TopnFilterContext {
    private final Map<TopN, TopnFilter> filters = Maps.newHashMap();

    /**
     * add topN filter
     */
    public void addTopnFilter(TopN topn, PhysicalRelation scan, Expression expr) {
        TopnFilter filter = filters.get(topn);
        if (filter == null) {
            filters.put(topn, new TopnFilter(topn, scan, expr));
        } else {
            filter.addTarget(scan, expr);
        }
    }

    public boolean isTopnFilterSource(TopN topn) {
        return filters.containsKey(topn);
    }

    public List<TopnFilter> getTopnFilters() {
        return Lists.newArrayList(filters.values());
    }

    /**
     * translate topn-filter
     */
    public void translateTarget(PhysicalRelation relation, ScanNode legacyScan,
            PlanTranslatorContext translatorContext) {
        for (TopnFilter filter : filters.values()) {
            if (filter.hasTargetRelation(relation)) {
                Expr expr = ExpressionTranslator.translate(filter.targets.get(relation), translatorContext);
                filter.legacyTargets.put(legacyScan, expr);
            }
        }
    }

    /**
     * translate topn-filter
     */
    public void translateSource(TopN topn, SortNode sortNode) {
        TopnFilter filter = filters.get(topn);
        if (filter == null) {
            return;
        }
        filter.legacySortNode = sortNode;
        sortNode.setUseTopnOpt(true);
        Preconditions.checkArgument(!filter.legacyTargets.isEmpty(), "missing targets: " + filter);
        for (ScanNode scan : filter.legacyTargets.keySet()) {
            scan.addTopnFilterSortNode(sortNode);
        }
    }

    /**
     * toString
     */
    public String toString() {
        StringBuilder builder = new StringBuilder("TopnFilterContext\n");
        String indent = "   ";
        String arrow = " -> ";
        builder.append("filters:\n");
        for (TopN topn : filters.keySet()) {
            builder.append(indent).append(arrow).append(filters.get(topn)).append("\n");
        }
        return builder.toString();
    }
}
