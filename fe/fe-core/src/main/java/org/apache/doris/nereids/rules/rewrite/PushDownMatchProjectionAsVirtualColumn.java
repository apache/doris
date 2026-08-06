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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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

    private boolean canPushDown(LogicalOlapScan scan) {
        boolean dupTblOrMOW = scan.getTable().getKeysType() == KeysType.DUP_KEYS
                || (scan.getTable().getTableProperty() != null
                    && scan.getTable().getTableProperty().getEnableUniqueKeyMergeOnWrite());
        return dupTblOrMOW;
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // Pattern 1: Project -> OlapScan
                logicalProject(logicalOlapScan().when(this::canPushDown))
                        .then(project -> {
                            LogicalOlapScan scan = project.child();
                            return pushDown(project, scan, newScan -> newScan);
                        }).toRule(RuleType.PUSH_DOWN_MATCH_PROJECTION_AS_VIRTUAL_COLUMN),
                // Pattern 2: Project -> Filter -> OlapScan
                logicalProject(logicalFilter(logicalOlapScan().when(this::canPushDown)))
                        .then(project -> {
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            return pushDown(project, scan,
                                    newScan -> filter.withChildren(newScan));
                        }).toRule(RuleType.PUSH_DOWN_MATCH_PROJECTION_AS_VIRTUAL_COLUMN)
        );
    }

    /**
     * Extract MATCH projections and push them as virtual columns on the scan.
     * @param childRebuilder rebuilds the project's child tree with the new scan
     */
    private LogicalProject<?> pushDown(
            LogicalProject<?> project, LogicalOlapScan scan,
            Function<LogicalOlapScan, ? extends Plan> childRebuilder) {
        List<NamedExpression> projections = project.getProjects();
        List<NamedExpression> virtualColumns = new ArrayList<>();
        Map<Expression, Expression> replaceMap = new HashMap<>();

        for (NamedExpression projection : projections) {
            Expression matchExpr = unwrapMatch(projection);
            if (matchExpr != null && !replaceMap.containsKey(matchExpr)) {
                Alias alias = new Alias(matchExpr);
                replaceMap.put(matchExpr, alias.toSlot());
                virtualColumns.add(alias);
            }
        }

        if (virtualColumns.isEmpty()) {
            return null;
        }

        ImmutableList.Builder<NamedExpression> newProjections = ImmutableList.builder();
        for (NamedExpression projection : projections) {
            Expression matchExpr = unwrapMatch(projection);
            if (matchExpr != null && replaceMap.containsKey(matchExpr)) {
                Expression slot = replaceMap.get(matchExpr);
                if (projection instanceof Alias) {
                    newProjections.add(new Alias(((Alias) projection).getExprId(),
                            slot, ((Alias) projection).getName()));
                } else {
                    newProjections.add((NamedExpression) slot);
                }
            } else {
                newProjections.add(projection);
            }
        }

        LogicalOlapScan newScan = scan.appendVirtualColumns(virtualColumns);
        return (LogicalProject<?>) project.withProjectsAndChild(
                newProjections.build(), childRebuilder.apply(newScan));
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
