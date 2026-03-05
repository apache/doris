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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Push down MATCH expressions in projections as virtual columns on OlapScan.
 * This allows the BE to evaluate MATCH using inverted index during scan.
 *
 * Example transformation:
 * Before:
 * Project[a, b, col MATCH_ANY 'hello']
 * └── OlapScan[table]
 *
 * After:
 * Project[a, b, virtual_slot_ref]
 * └── OlapScan[table, virtual_columns=[(col MATCH_ANY 'hello') as alias]]
 */
public class PushDownMatchProjectionAsVirtualColumn implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalProject(logicalOlapScan()
                        .when(scan -> {
                            boolean dupTblOrMOW = scan.getTable().getKeysType() == KeysType.DUP_KEYS
                                    || scan.getTable().getTableProperty().getEnableUniqueKeyMergeOnWrite();
                            return dupTblOrMOW && scan.getVirtualColumns().isEmpty();
                        }))
                        .then(project -> {
                            LogicalOlapScan scan = project.child();
                            return pushDown(project, scan);
                        }).toRule(RuleType.PUSH_DOWN_MATCH_PROJECTION_AS_VIRTUAL_COLUMN)
        );
    }

    private LogicalProject<?> pushDown(
            LogicalProject<LogicalOlapScan> project, LogicalOlapScan scan) {
        List<NamedExpression> projections = project.getProjects();
        List<NamedExpression> virtualColumns = new ArrayList<>();
        // Map from original Match expression to its alias slot
        Map<Expression, Expression> replaceMap = new HashMap<>();

        for (NamedExpression projection : projections) {
            Expression matchExpr = unwrapMatch(projection);
            if (matchExpr != null && matchExpr.child(0) instanceof SlotReference
                    && !replaceMap.containsKey(matchExpr)) {
                Alias alias = new Alias(matchExpr);
                replaceMap.put(matchExpr, alias.toSlot());
                virtualColumns.add(alias);
            }
        }

        if (virtualColumns.isEmpty()) {
            return null;
        }

        // Build new projections replacing Match expressions with virtual column slots
        ImmutableList.Builder<NamedExpression> newProjections = ImmutableList.builder();
        for (NamedExpression projection : projections) {
            Expression matchExpr = unwrapMatch(projection);
            if (matchExpr != null && replaceMap.containsKey(matchExpr)) {
                Expression slot = replaceMap.get(matchExpr);
                if (projection instanceof Alias) {
                    // Preserve the original alias name
                    newProjections.add(new Alias(((Alias) projection).getExprId(),
                            slot, ((Alias) projection).getName()));
                } else {
                    newProjections.add((NamedExpression) slot);
                }
            } else {
                newProjections.add(projection);
            }
        }

        LogicalOlapScan newScan = scan.withVirtualColumns(virtualColumns);
        return (LogicalProject<?>) project.withProjectsAndChild(
                newProjections.build(), newScan);
    }

    /**
     * Unwrap a Match expression from a projection.
     * Returns the Match expression if the projection is a Match directly or an Alias wrapping a Match.
     * Returns null otherwise.
     */
    private Expression unwrapMatch(NamedExpression projection) {
        if (projection instanceof Alias && ((Alias) projection).child() instanceof Match) {
            return ((Alias) projection).child();
        }
        return null;
    }
}
