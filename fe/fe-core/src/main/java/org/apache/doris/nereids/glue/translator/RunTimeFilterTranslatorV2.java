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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.nereids.processor.post.runtimefilterv2.RuntimeFilterV2;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * RunTimeFilterTranslatorV2
 */
public class RunTimeFilterTranslatorV2 {
    public static RunTimeFilterTranslatorV2 INSTANCE = new RunTimeFilterTranslatorV2();

    /**
     * createLegacyRuntimeFilters
     */
    public void createLegacyRuntimeFilters(PlanNode sourceNode,
            List<RuntimeFilterV2> filters, PlanTranslatorContext ctx) {
        List<RuntimeFilterV2> filtersToTranslate = Lists.newArrayList(filters);
        Set<Integer> ignoreRuntimeFilterIds = ConnectContext.get() != null
                ? ConnectContext.get().getSessionVariable().getIgnoredRuntimeFilterIds()
                : new HashSet<>();
        while (!filtersToTranslate.isEmpty()) {
            List<RuntimeFilterV2> translateRound = Lists.newArrayListWithCapacity(filtersToTranslate.size());
            List<RuntimeFilterV2> otherRound = Lists.newArrayListWithCapacity(filtersToTranslate.size());
            RuntimeFilterV2 head = filtersToTranslate.get(0);
            if (!ignoreRuntimeFilterIds.contains(head.getRuntimeFilterId().asInt())) {
                translateRound.add(head);
            }
            for (int i = 1; i < filtersToTranslate.size(); i++) {
                if (!ignoreRuntimeFilterIds.contains(filtersToTranslate.get(i).getRuntimeFilterId().asInt())) {
                    if (head.getSourceExpression().equals(filtersToTranslate.get(i).getSourceExpression())
                            && head.getType() == filtersToTranslate.get(i).getType()) {
                        translateRound.add(filtersToTranslate.get(i));
                    } else {
                        otherRound.add(filtersToTranslate.get(i));
                    }
                }
            }
            if (!translateRound.isEmpty()) {
                translateRuntimeFilterGroup(sourceNode, translateRound, ctx);
            }
            filtersToTranslate = otherRound;
        }

    }

    /**
     * a group of RFs if their source and type are the same, but their targets are
     * different.
     * example:
     * rf1[bloom](a->T1.b) rf2[bloom](a->T2.c) rf3[min_max](a->t3.d)
     * rf1 and rf2 are in one group, but rf3 is not
     *
     */
    private void translateRuntimeFilterGroup(PlanNode sourceNode,
            List<RuntimeFilterV2> filters, PlanTranslatorContext ctx) {
        if (filters.isEmpty()) {
            return;
        }

        RuntimeFilterV2 head = filters.get(0);

        Expr srcExpr = ExpressionTranslator.translate(head.getSourceExpression(), ctx);

        List<RuntimeFilterTarget> targets = new ArrayList<>();
        for (RuntimeFilterV2 filter : filters) {
            Expr targetExpr = filter.getLegacyTargetExpr();
            if (!srcExpr.getType().equals(targetExpr.getType())) {
                targetExpr = new CastExpr(srcExpr.getType(), targetExpr);
            }
            targets.add(new RuntimeFilterTarget(filter.getLegacyTargetNode(), targetExpr));
        }

        RuntimeFilter legacyFilter = new RuntimeFilter(
                head.getId(),
                sourceNode,
                srcExpr,
                head.getExprOrder(),
                targets,
                head.getType(),
                head.getBuildNdvOrRowCount(),
                head.getTMinMaxRuntimeFilterType());

        ctx.getRuntimeFilterV2Context().addLegacyRuntimeFilter(legacyFilter);

        // finalize
        legacyFilter.assignToPlanNodes();
        legacyFilter.extractTargetsPosition();
        legacyFilter.markFinalized();
    }

}
