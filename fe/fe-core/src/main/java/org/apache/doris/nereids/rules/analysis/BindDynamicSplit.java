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

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.SplitColumnInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalDynamicSplit;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This class defines the rule to bind a dynamic split operation in the query plan.
 */
public class BindDynamicSplit implements AnalysisRuleFactory {
    private static final Logger LOG = LoggerFactory.getLogger(BindDynamicSplit.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                //LogicalDynamicSplit -> LogicalFilter -> LogicalCatalogRelation
                RuleType.BIND_DYNAMIC_SPLIT.build(logicalDynamicSplit(logicalFilter(logicalRelation()))
                        .thenApply(ctx -> {

                            Plan result = ctx.root.child();
                            LogicalDynamicSplit<LogicalFilter<LogicalRelation>> logicalDynamicSplit = ctx.root;
                            LogicalRelation logicalRelation = (LogicalRelation) ((LogicalFilter<?>)
                                    logicalDynamicSplit.child())
                                    .child();
                            return rewritePlan(logicalRelation, result, logicalDynamicSplit);
                        })),
                //LogicalDynamicSplit -> LogicalProject -> LogicalFilter -> LogicalCatalogRelation
                RuleType.BIND_DYNAMIC_SPLIT.build(logicalDynamicSplit(logicalProject(logicalFilter(logicalRelation())))
                        .thenApply(ctx -> {

                            Plan result = ctx.root.child();
                            LogicalDynamicSplit logicalDynamicSplit = ctx.root;
                            LogicalRelation logicalRelation = (LogicalRelation) ((LogicalProject<?>)
                                    ((LogicalFilter<?>) logicalDynamicSplit.child()).child()).child();
                            return rewritePlan(logicalRelation, result, logicalDynamicSplit);
                        })),
                //LogicalDynamicSplit -> LogicalSubQueryAlias -> LogicalView
                RuleType.BIND_DYNAMIC_SPLIT.build(logicalDynamicSplit(logicalSubQueryAlias(logicalView()))
                        .thenApply(ctx -> {

                            Plan result = ctx.root.child();
                            LogicalDynamicSplit<LogicalSubQueryAlias<LogicalView<Plan>>> logicalDynamicSplit = ctx.root;
                            LogicalView logicalView = (LogicalView) ((LogicalSubQueryAlias<?>) logicalDynamicSplit
                                    .child()).child();

                            return rewritePlan(logicalView, result, logicalDynamicSplit);
                        })),
                // LogicalDynamicSplit -> LogicalCatalogRelation
                RuleType.BIND_DYNAMIC_SPLIT.build(logicalDynamicSplit(logicalRelation()).thenApply(ctx -> {
                    Plan result = ctx.root.child();
                    LogicalDynamicSplit<LogicalRelation> logicalDynamicSplit = ctx.root;
                    LogicalRelation logicalCatalogRelation = ctx.root.child();
                    if (null == logicalCatalogRelation) {
                        return result;
                    }
                    return rewritePlan(logicalCatalogRelation, result, logicalDynamicSplit);
                })));
    }

    private LogicalCatalogRelation convertAndCheckLogicalRelation(LogicalRelation logicalRelation) {
        if (logicalRelation instanceof LogicalCatalogRelation) {
            return (LogicalCatalogRelation) logicalRelation;
        }
        return null;
    }

    /**
     * Applies a filter to the plan based on the dynamic split criteria.
     * This method is shared between LogicalView and LogicalRelation to avoid code duplication.
     *
     * @param result              The original plan that needs to be rewritten.
     * @param relationChild       The child of the relation node.
     *                            Will add filter to this node if it matches the split criteria.
     * @param logicalDynamicSplit The dynamic split information which contains the range and column name.
     * @param catalogTable        The catalog table from which the slot reference will be retrieved.
     * @return The rewritten plan with the added filter.
     */
    private Plan rewritePlanWithFilter(Plan result, Plan relationChild, LogicalDynamicSplit logicalDynamicSplit,
                                       TableIf catalogTable) {
        logicalDynamicSplit.setReplaced(true);
        Range range = logicalDynamicSplit.getRange();
        SplitColumnInfo splitColumnInfo = logicalDynamicSplit.getSplitColumnInfo();
        String splitColumnName = splitColumnInfo.getColumnName();
        TableName splitTable = splitColumnInfo.getTableNameInfo().transferToTableName();

        // Verify if the catalog table matches the split table information.
        if (!catalogTable.getName().equals(splitTable.getTbl())
                || !catalogTable.getDatabase().getFullName().equals(splitTable.getDb())
                || !catalogTable.getDatabase().getCatalog().getName().equals(splitTable.getCtl())) {
            return result;
        }

        SlotReference slotReference = getSlotReference(relationChild.getOutput(), splitColumnName);
        if (slotReference == null) {
            logicalDynamicSplit.setReplaced(false);
            LOG.warn("Cannot find the split column {} in the table {}", splitColumnName, catalogTable.getName());
            return result;
        }

        // Create the filter conditions based on the range of the dynamic split.
        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(slotReference, Literal.of(range.getMinimum()));
        LessThanEqual lessThanEqual = new LessThanEqual(slotReference, Literal.of(range.getMaximum()));
        LogicalFilter logicalFilter = new LogicalFilter<>(ImmutableSet.of(
                greaterThanEqual, lessThanEqual), relationChild);

        // Rewrite the plan by replacing the catalog table node with the logical filter.
        result = result.rewriteUp(node -> {
            if (node.equals(catalogTable)) {
                return logicalFilter;
            }
            return node;
        });

        return result;
    }

    /**
     * Rewrites the plan for LogicalView by applying the dynamic split filter.
     *
     * @param logicalView         The logical view to apply the filter on.
     * @param result              The original plan.
     * @param logicalDynamicSplit The dynamic split information.
     * @return The rewritten plan with the filter applied.
     */
    private Plan rewritePlan(LogicalView logicalView, Plan result, LogicalDynamicSplit logicalDynamicSplit) {
        return rewritePlanWithFilter(result, logicalView, logicalDynamicSplit, logicalView.getView());
    }

    /**
     * Rewrites the plan for LogicalRelation by applying the dynamic split filter.
     *
     * @param logicalRelation     The logical relation to apply the filter on.
     * @param result              The original plan.
     * @param logicalDynamicSplit The dynamic split information.
     * @return The rewritten plan with the filter applied.
     */
    private Plan rewritePlan(LogicalRelation logicalRelation, Plan result, LogicalDynamicSplit logicalDynamicSplit) {
        LogicalCatalogRelation catalogRelation = convertAndCheckLogicalRelation(logicalRelation);
        if (catalogRelation == null) {
            return result;
        }
        return rewritePlanWithFilter(result, catalogRelation, logicalDynamicSplit, catalogRelation.getTable());
    }

    private SlotReference getSlotReference(List<Slot> outputs, String splitColumnName) {
        for (Slot slot : outputs) {
            if (slot.getName().equalsIgnoreCase(splitColumnName)) {
                return (SlotReference) slot;
            }
        }
        return null;
    }
}
