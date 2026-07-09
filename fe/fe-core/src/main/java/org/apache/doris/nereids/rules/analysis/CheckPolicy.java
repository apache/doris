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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.datasource.ConnectorExpressionToNereidsConverter;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy.RelatedPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHudiScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CheckPolicy.
 */
public class CheckPolicy implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.CHECK_ROW_POLICY.build(
                        logicalCheckPolicy(any().when(child -> !(child instanceof UnboundRelation))).thenApply(ctx -> {
                            LogicalCheckPolicy<Plan> checkPolicy = ctx.root;
                            LogicalFilter<Plan> upperFilter = null;
                            Plan upAgg = null;

                            Plan child = checkPolicy.child();
                            // Because the unique table will automatically include a filter condition
                            if ((child instanceof LogicalFilter)) {
                                upperFilter = (LogicalFilter) child;
                                if (child.child(0) instanceof LogicalRelation) {
                                    child = child.child(0);
                                } else if (child.child(0) instanceof LogicalAggregate
                                        && child.child(0).child(0) instanceof LogicalRelation) {
                                    upAgg = child.child(0);
                                    child = child.child(0).child(0);
                                }
                            }
                            if ((child instanceof LogicalAggregate) && child.child(0) instanceof LogicalRelation) {
                                upAgg = child;
                                child = child.child(0);
                            }
                            if (!(child instanceof LogicalRelation || isView(child))
                                    || ctx.connectContext.getSessionVariable().isPlayNereidsDump()) {
                                return ctx.root.child();
                            }
                            LogicalPlan relation = child instanceof LogicalSubQueryAlias ? (LogicalPlan) child.child(0)
                                    : (LogicalPlan) child;
                            Set<Expression> combineFilter = new LinkedHashSet<>();

                            // replace incremental params as AND expression
                            if (relation instanceof LogicalHudiScan) {
                                // Legacy hudi-on-HMS incremental read (LogicalHudiScan); deleted at the cutover.
                                LogicalHudiScan hudiScan = (LogicalHudiScan) relation;
                                if (hudiScan.getTable() instanceof HMSExternalTable) {
                                    combineFilter.addAll(hudiScan.generateIncrementalExpression(
                                            hudiScan.getLogicalProperties().getOutput()));
                                }
                            } else if (relation instanceof LogicalFileScan
                                    && ((LogicalFileScan) relation).getTable() instanceof PluginDrivenExternalTable
                                    && ((LogicalFileScan) relation).getScanParams().isPresent()) {
                                // Neutral synthetic-predicate injection for an SPI-driven (plugin) scan: the
                                // connector supplies a residual predicate the engine must apply (e.g. a hudi @incr
                                // _hoodie_commit_time commit-time window), which fe-core reverse-converts into an
                                // AND filter WITHOUT branching on the source. iceberg/paimon/... return empty, so
                                // nothing is added and the plan stays byte-identical (the iron-rule guarantee).
                                combineFilter.addAll(collectConnectorSyntheticPredicates(
                                        (LogicalFileScan) relation, ctx.cascadesContext.getStatementContext()));
                            }

                            RelatedPolicy relatedPolicy = checkPolicy.findPolicy(relation, ctx.cascadesContext);
                            relatedPolicy.rowPolicyFilter.ifPresent(expression -> combineFilter.addAll(
                                    ExpressionUtils.extractConjunctionToSet(expression)));
                            Plan result = upAgg != null ? upAgg.withChildren(child) : child;
                            if (upperFilter != null) {
                                combineFilter.addAll(upperFilter.getConjuncts());
                            }
                            if (!combineFilter.isEmpty()) {
                                result = new LogicalFilter<>(combineFilter, result);
                            }
                            if (relatedPolicy.dataMaskProjects.isPresent()) {
                                result = new LogicalProject<>(relatedPolicy.dataMaskProjects.get(), result);
                            }

                            return result;
                        })
                )
        );
    }

    /**
     * Collects the connector's synthetic scan predicates for a plugin {@link LogicalFileScan} and reverse-converts
     * them into bound Nereids conjuncts. The connector-neutral {@link ConnectorExpression}s (e.g. a hudi @incr
     * {@code _hoodie_commit_time} window) come from the SPI; fe-core only binds their column refs to the scan's
     * output slots by name and maps the node shapes back to Nereids — it never branches on the source. Empty for
     * every non-opting connector (iceberg/paimon/...) and every non-incremental read, so the plan is unchanged.
     */
    private Set<Expression> collectConnectorSyntheticPredicates(LogicalFileScan scan,
            StatementContext statementContext) {
        PluginDrivenExternalTable table = (PluginDrivenExternalTable) scan.getTable();
        // The MVCC snapshot resolved at analysis time (StatementContext.loadSnapshots) carries the
        // connector-resolved window — the SAME single resolution the scan-time applySnapshot threads onto the
        // handle, so the row filter and the file selection can never diverge.
        MvccSnapshot snapshot = statementContext
                .getSnapshot(table, scan.getTableSnapshot(), scan.getScanParams())
                .orElse(null);
        if (snapshot == null) {
            return Collections.emptySet();
        }
        List<ConnectorExpression> predicates = table.getSyntheticScanPredicates(snapshot);
        if (predicates.isEmpty()) {
            return Collections.emptySet();
        }
        Map<String, SlotReference> boundSlots = new HashMap<>();
        for (Slot slot : scan.getLogicalProperties().getOutput()) {
            if (slot instanceof SlotReference) {
                boundSlots.put(slot.getName(), (SlotReference) slot);
            }
        }
        Set<Expression> result = new LinkedHashSet<>();
        for (ConnectorExpression predicate : predicates) {
            result.add(ConnectorExpressionToNereidsConverter.convert(predicate, boundSlots));
        }
        return result;
    }

    // logicalView() or logicalSubQueryAlias(logicalView())
    private boolean isView(Plan plan) {
        if (plan instanceof LogicalView) {
            return true;
        }
        if (plan instanceof LogicalSubQueryAlias && plan.children().size() > 0 && plan.child(
                0) instanceof LogicalView) {
            return true;
        }
        return false;
    }
}
