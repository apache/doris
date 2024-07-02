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
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalDynamicSplit;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
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
                // LogicalDynamicSplit
                RuleType.BIND_DYNAMIC_SPLIT.build(logicalDynamicSplit().thenApply(ctx -> {

                    Plan result = ctx.root.child();
                    LogicalDynamicSplit logicalDynamicSplit = ctx.root;
                    LogicalCatalogRelation logicalCatalogRelation = checkAndReturnCatalogRelation(logicalDynamicSplit);
                    if (null == logicalCatalogRelation) {
                        return result;
                    }
                    return rewritePlan(logicalCatalogRelation, result, logicalDynamicSplit);
                })));
    }

    private LogicalCatalogRelation checkAndReturnCatalogRelation(LogicalDynamicSplit logicalDynamicSplit) {
        // LogicalDynamicSplit -> LogicalCatalogRelation
        if (logicalDynamicSplit.child() instanceof LogicalCatalogRelation) {
            return (LogicalCatalogRelation) logicalDynamicSplit.child();
        }
        //LogicalDynamicSplit -> LogicalFilter -> LogicalCatalogRelation
        if (logicalDynamicSplit.child() instanceof LogicalFilter) {
            LogicalFilter logicalFilter = (LogicalFilter) logicalDynamicSplit.child();
            if (logicalFilter.child() instanceof LogicalCatalogRelation) {
                return (LogicalCatalogRelation) logicalFilter.child();
            }
        }

        //LogicalDynamicSplit -> LogicalProject -> LogicalFilter -> LogicalCatalogRelation
        if (logicalDynamicSplit.child() instanceof LogicalProject) {
            if (((LogicalProject<?>) logicalDynamicSplit.child()).child() instanceof LogicalFilter) {
                LogicalFilter logicalFilter = (LogicalFilter) ((LogicalProject) logicalDynamicSplit.child()).child();
                if (logicalFilter.child() instanceof LogicalCatalogRelation) {
                    return (LogicalCatalogRelation) logicalFilter.child();
                }
            }
        }
        // todo wait view refactor
        //LogicalDynamicSplit -> LogicalSubQueryAlias -> LogicalView -> LogicalAggregate
        // -> LogicalFilter -> LogicalCatalogRelation
        if (logicalDynamicSplit.child() instanceof LogicalSubQueryAlias) {
            LogicalSubQueryAlias logicalSubQueryAlias = (LogicalSubQueryAlias) logicalDynamicSplit.child();
            if (logicalSubQueryAlias.child() instanceof LogicalView) {
                LogicalView logicalView = (LogicalView) logicalSubQueryAlias.child();

                if (logicalView.child() instanceof LogicalAggregate) {
                    LogicalAggregate logicalAggregate = (LogicalAggregate) logicalView.child();
                    if (logicalAggregate.child() instanceof LogicalFilter) {
                        LogicalFilter logicalFilter = (LogicalFilter) logicalAggregate.child();
                        if (logicalFilter.child() instanceof LogicalCatalogRelation) {
                            return (LogicalCatalogRelation) logicalFilter.child();
                        }
                    }
                }
            }
        }
        // LogicalDynamicSplit -> LogicalSubQueryAlias -> LogicalView -> LogicalProject -> LogicalCatalogRelation
        if (logicalDynamicSplit.child() instanceof LogicalSubQueryAlias) {
            LogicalSubQueryAlias logicalSubQueryAlias = (LogicalSubQueryAlias) logicalDynamicSplit.child();
            if (logicalSubQueryAlias.child() instanceof LogicalView) {
                LogicalView logicalView = (LogicalView) logicalSubQueryAlias.child();
                if (logicalView.child() instanceof LogicalProject) {
                    LogicalProject logicalProject = (LogicalProject) logicalView.child();
                    if (logicalProject.child() instanceof LogicalCatalogRelation) {
                        return (LogicalCatalogRelation) logicalProject.child();
                    }
                }
            }
        }
        LOG.warn("The child of LogicalDynamicSplit is not LogicalCatalogRelation, but {}",
                logicalDynamicSplit.child().getClass().getSimpleName());
        return null;
    }

    private Plan rewritePlan(LogicalCatalogRelation catalogRelation, Plan result,
                             LogicalDynamicSplit logicalDynamicSplit) {
        Range range = logicalDynamicSplit.getRange();
        SplitColumnInfo splitColumnInfo = logicalDynamicSplit.getSplitColumnInfo();
        String splitColumnName = splitColumnInfo.getColumnName();
        TableName splitTable = splitColumnInfo.getTableNameInfo().transferToTableName();
        TableIf table = catalogRelation.getTable();

        if (!table.getName().equals(splitTable.getTbl())) {
            return catalogRelation;
        }
        if (!table.getDatabase().getFullName().equals(splitTable.getDb())) {
            return catalogRelation;
        }
        if (!table.getDatabase().getCatalog().getName().equals(splitTable.getCtl())) {
            return catalogRelation;
        }
        SlotReference slotReference = null;
        for (Slot slot : catalogRelation.getOutputSet()) {
            if (slot.getName().equalsIgnoreCase(splitColumnName)) {
                slotReference = (SlotReference) slot;
            }
        }
        if (null == slotReference) {
            LOG.warn("Can not find the split column {} in the table {}", splitColumnName, table.getName());
            return catalogRelation;
        }
        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(slotReference,
                Literal.of(range.getMinimum()));
        LessThanEqual lessThanEqual = new LessThanEqual(slotReference, Literal.of(range.getMaximum()));
        logicalDynamicSplit.setReplaced(Boolean.TRUE);
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
}
