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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.commands.info.SplitColumnInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalDynamicSplit;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.Range;

import java.util.List;

/**
 * This class defines the rule to bind a dynamic split operation in the query plan.
 */
public class BindDynamicSplit implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(

                RuleType.BIND_DYNAMIC_SPLIT.build(logicalDynamicSplit().thenApply(ctx -> {
                    if (!(ctx.root.child() instanceof LogicalCatalogRelation)) {
                        return ctx.root;
                    }

                    LogicalDynamicSplit logicalDynamicSplit = ctx.root;
                    Range range = logicalDynamicSplit.getRange();
                    SplitColumnInfo splitColumnInfo = logicalDynamicSplit.getSplitColumnInfo();
                    String splitColumnName = splitColumnInfo.getColumnName();
                    TableName splitTable = splitColumnInfo.getTableNameInfo().transferToTableName();
                    LogicalCatalogRelation catalogRelation = (LogicalCatalogRelation) ctx.root.child();
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
                        throw new AnalysisException(
                                String.format("Column %s not found in table %s, binding expr fail!",
                                        splitColumnName, table.getName()));
                    }
                    GreaterThanEqual greaterThanEqual = new GreaterThanEqual(slotReference,
                            Literal.of(range.getMinimum()));
                    LessThanEqual lessThanEqual = new LessThanEqual(slotReference, Literal.of(range.getMaximum()));
                    logicalDynamicSplit.setReplaced(Boolean.TRUE);
                    return new LogicalFilter<>(ImmutableSet.of(greaterThanEqual, lessThanEqual), catalogRelation);
                })));
    }
}
