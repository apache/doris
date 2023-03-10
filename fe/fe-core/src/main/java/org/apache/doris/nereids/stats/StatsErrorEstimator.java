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

package org.apache.doris.nereids.stats;

import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TUniqueId;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Used to estimate the bias of stats estimation.
 */
public class StatsErrorEstimator {
    private Map<Integer, Pair<Double, Double>> legacyPlanIdToPhysicalPlan;

    public StatsErrorEstimator() {
        legacyPlanIdToPhysicalPlan = new HashMap<>();
    }

    public void updateLegacyPlanIdToPhysicalPlan(PlanNode planNode, AbstractPlan physicalPlan) {
        legacyPlanIdToPhysicalPlan.put(planNode.getId().asInt(), Pair.of(physicalPlan.getStats().getRowCount(),
                (double) 0));
    }

    /**
     *  Q-error:
     *      q = max_{i=1}^{n}(max(\frac{b^\prime}{b}, \frac{b}{b^\prime})
     */
    public double calculateQError() {
        double qError = Double.NEGATIVE_INFINITY;
        for (Entry<Integer, Pair<Double, Double>> entry : legacyPlanIdToPhysicalPlan.entrySet()) {
            double exactReturnedRows = entry.getValue().second;
            double estimateReturnedRows = entry.getValue().first;
            qError = Math.max(qError,
                    Math.max(exactReturnedRows / oneIfZero(estimateReturnedRows),
                            estimateReturnedRows / oneIfZero(exactReturnedRows)));
        }
        return qError;
    }

    /**
     * Update extract returned rows incrementally, since there may be many execution instances of plan fragment.
     */
    public void updateExactReturnedRows(TReportExecStatusParams tReportExecStatusParams) {
        TUniqueId tUniqueId = tReportExecStatusParams.query_id;
        for (TRuntimeProfileNode runtimeProfileNode : tReportExecStatusParams.profile.nodes) {
            String name = runtimeProfileNode.name;
            int planId = extractPlanNodeIdFromName(name);
            if (planId == -1) {
                continue;
            }
            double rowsReturned = runtimeProfileNode.counters.stream()
                    .filter(p -> p.name.equals("RowsReturned")).mapToDouble(p -> (double) p.getValue()).sum();
            Pair<Double, Double> pair = legacyPlanIdToPhysicalPlan.get(planId);
            pair.second = pair.second + rowsReturned;
        }
        double qError = calculateQError();
        ProfileManager.getInstance()
                .setQErrorToProfileElementObject(DebugUtil.printId(tUniqueId), qError);
    }

    /**
     * TODO: The execution report from BE doesn't have any schema, so we have to use regex to extract the plan node id.
     */
    private int extractPlanNodeIdFromName(String name) {
        Pattern p = Pattern.compile("\\b(?!dst_id=)id=(\\d+)\\b");
        Matcher m = p.matcher(name);
        if (!m.find()) {
            return -1;
        }
        return Integer.parseInt(m.group(1));
    }

    private Double extractRowsReturned(String rowsReturnedStr) {
        if (rowsReturnedStr == null) {
            return 0.0;
        }
        Pattern p = Pattern.compile("\\((\\d+)\\)");
        Matcher m = p.matcher(rowsReturnedStr);
        if (!m.find()) {
            return 0.0;
        }
        return Double.parseDouble(m.group(1));
    }

    private double oneIfZero(double d) {
        return d == 0.0 ? 1.0 : d;
    }
}
