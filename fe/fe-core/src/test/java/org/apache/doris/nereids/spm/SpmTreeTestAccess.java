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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintLeading;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.properties.SelectHintUseMv;
import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SPMPlan2SQLBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Test-side adapters to the replay summaries and combined freeze guard. */
public final class SpmTreeTestAccess {
    private SpmTreeTestAccess() {
    }

    public static boolean rowLimitsWithin(Plan replayed, Plan userPlan) {
        return SPMPlanTreeSupport.rowLimitsWithin(
                new SPMPlanTreeSupport.ReplayContracts(replayed),
                new SPMPlanTreeSupport.ReplayContracts(userPlan));
    }

    public static boolean rowLimitsEquivalentModuloOccurrence(Plan left, Plan right) {
        return SPMPlanTreeSupport.rowLimitsEquivalentModuloOccurrence(
                new SPMPlanTreeSupport.ReplayContracts(left),
                new SPMPlanTreeSupport.ReplayContracts(right));
    }

    public static boolean rowLimitsSurviveReplay(Plan replayed, Plan userPlan) {
        return SPMPlanTreeSupport.rowLimitsSurviveReplay(
                new SPMPlanTreeSupport.ReplayContracts(replayed),
                new SPMPlanTreeSupport.ReplayContracts(userPlan));
    }

    public static boolean topCapContractPreserved(Plan replayed, Plan userPlan) {
        return SPMPlanTreeSupport.topCapContractPreserved(
                new SPMPlanTreeSupport.ReplayContracts(replayed),
                new SPMPlanTreeSupport.ReplayContracts(userPlan));
    }

    public static boolean onlyTopCap(Plan plan) {
        return SPMPlanTreeSupport.onlyTopCap(
                new SPMPlanTreeSupport.ReplayContracts(plan));
    }

    public static boolean replayCapsJustifiedByCallerTopLimit(Plan replayed, Plan userPlan) {
        return SPMPlanTreeSupport.replayCapsJustifiedByCallerTopLimit(
                new SPMPlanTreeSupport.ReplayContracts(replayed),
                new SPMPlanTreeSupport.ReplayContracts(userPlan));
    }

    public static boolean orderContractPreserved(Plan replayed, Plan userPlan) {
        return SPMPlanTreeSupport.orderContractPreserved(
                new SPMPlanTreeSupport.ReplayContracts(replayed),
                new SPMPlanTreeSupport.ReplayContracts(userPlan));
    }

    public static String selectHintsFingerprint(Plan parsed) {
        if (parsed == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(parsed, (Plan node) -> {
            if (node instanceof LogicalSelectHint) {
                for (SelectHint hint : ((LogicalSelectHint<?>) node).getHints()) {
                    appendSelectHint(sb, hint);
                }
            }
        });
        return sb.toString();
    }

    private static void appendSelectHint(StringBuilder sb, SelectHint hint) {
        sb.append(hint.getClass().getSimpleName()).append('(')
                .append(hint.getHintName().toLowerCase()).append('|');
        if (hint instanceof SelectHintSetVar) {
            // sorted case-insensitive name=value rendering: the structural comparison
            // matches the map case-insensitively, so the rendering must not depend on
            // the map's iteration order
            List<String> rendered = new ArrayList<>();
            for (Map.Entry<String, Optional<String>> entry
                    : ((SelectHintSetVar) hint).getParameters().entrySet()) {
                rendered.add(entry.getKey().toLowerCase() + '='
                        + entry.getValue().orElse("null"));
            }
            Collections.sort(rendered);
            sb.append(String.join(",", rendered));
        } else if (hint instanceof SelectHintUseMv) {
            SelectHintUseMv mv = (SelectHintUseMv) hint;
            sb.append(mv.isUseMv()).append('|').append(mv.getTables());
        } else if (hint instanceof SelectHintLeading) {
            SelectHintLeading leading = (SelectHintLeading) hint;
            sb.append(leading.getParameters()).append('|').append(leading.getStrToHint());
        } else {
            sb.append(hint);
        }
        sb.append(");");
    }

    public static String digest(Expression expression) {
        return new SPMExprSqlBuilder().print(expression, new SQLRelation());
    }

    public static void rejectUnsupportedExpressions(Plan plan) {
        try {
            java.lang.reflect.Method method = SPMPlan2SQLBuilder.class
                    .getDeclaredMethod("rejectUnsupportedExpressions", Plan.class);
            method.setAccessible(true);
            method.invoke(null, plan);
        } catch (java.lang.reflect.InvocationTargetException e) {
            throw (RuntimeException) e.getCause();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }
}
