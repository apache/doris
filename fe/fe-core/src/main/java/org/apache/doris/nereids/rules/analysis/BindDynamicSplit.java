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
import org.apache.doris.catalog.View;
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

    private Plan rewritePlan(LogicalView logicalView, Plan result,
                             LogicalDynamicSplit logicalDynamicSplit) {
        Range range = logicalDynamicSplit.getRange();
        SplitColumnInfo splitColumnInfo = logicalDynamicSplit.getSplitColumnInfo();
        String splitColumnName = splitColumnInfo.getColumnName();
        TableName splitTable = splitColumnInfo.getTableNameInfo().transferToTableName();
        View view = logicalView.getView();
        logicalDynamicSplit.setReplaced(Boolean.TRUE);
        if (!view.getName().equalsIgnoreCase(splitTable.getTbl())) {
            return result;
        }
        if (!view.getDatabase().getFullName().equals(splitTable.getDb())) {
            return result;
        }
        if (!view.getDatabase().getCatalog().getName().equals(splitTable.getCtl())) {
            return result;
        }
        SlotReference slotReference = getSlotReference(logicalView.getOutput(), splitColumnName);
        if (null == slotReference) {
            logicalDynamicSplit.setReplaced(Boolean.FALSE);
            LOG.warn("Can not find the split column {} in the table {}", splitColumnName, splitColumnName);
            return result;
        }
        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(slotReference,
                Literal.of(range.getMinimum()));
        LessThanEqual lessThanEqual = new LessThanEqual(slotReference, Literal.of(range.getMaximum()));
        LogicalFilter logicalFilter = new LogicalFilter<>(ImmutableSet.of(greaterThanEqual, lessThanEqual),
                logicalView);
        result = result.rewriteUp(node -> {
            if (node instanceof LogicalView) {
                return logicalFilter;
            }
            return node;
        });
        return result;
    }

    private Plan rewritePlan(LogicalRelation logicalRelation, Plan result,
                             LogicalDynamicSplit logicalDynamicSplit) {
        LogicalCatalogRelation catalogRelation = convertAndCheckLogicalRelation(logicalRelation);
        if (null == catalogRelation) {
            return result;
        }
        Range range = logicalDynamicSplit.getRange();
        SplitColumnInfo splitColumnInfo = logicalDynamicSplit.getSplitColumnInfo();
        String splitColumnName = splitColumnInfo.getColumnName();
        TableName splitTable = splitColumnInfo.getTableNameInfo().transferToTableName();
        TableIf table = catalogRelation.getTable();
        logicalDynamicSplit.setReplaced(Boolean.TRUE);
        if (!table.getName().equals(splitTable.getTbl())) {
            return result;
        }
        if (!table.getDatabase().getFullName().equals(splitTable.getDb())) {
            return result;
        }
        if (!table.getDatabase().getCatalog().getName().equals(splitTable.getCtl())) {
            return result;
        }
        SlotReference slotReference = getSlotReference(catalogRelation.getOutput(), splitColumnName);
        if (null == slotReference) {
            logicalDynamicSplit.setReplaced(Boolean.FALSE);
            LOG.warn("Can not find the split column {} in the table {}", splitColumnName, table.getName());
            return result;
        }
        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(slotReference,
                Literal.of(range.getMinimum()));
        LessThanEqual lessThanEqual = new LessThanEqual(slotReference, Literal.of(range.getMaximum()));

        LogicalFilter logicalFilter = new LogicalFilter<>(ImmutableSet.of(greaterThanEqual, lessThanEqual),
                catalogRelation);
        result = result.rewriteUp(node -> {
            if (node instanceof LogicalCatalogRelation) {
                return logicalFilter;
            }
            return node;
        });

        return result;
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
