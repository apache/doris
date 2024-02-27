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

package org.apache.doris.nereids.properties;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Table level function dependence items.
 */
public class TableFdItem extends FdItem {

    private ImmutableSet<TableIf> childTables;

    public TableFdItem(ImmutableSet<SlotReference> parentExprs, boolean isUnique,
            boolean isCandidate, ImmutableSet<TableIf> childTables) {
        super(parentExprs, isUnique, isCandidate);
        this.childTables = ImmutableSet.copyOf(childTables);
    }

    @Override
    public boolean checkExprInChild(SlotReference slot, LogicalPlan childPlan) {
        NamedExpression slotInChild = null;
        // find in child project based on exprid matching
        List<NamedExpression> exprList = ((LogicalProject) childPlan).getProjects();
        for (NamedExpression expr : exprList) {
            if (expr.getExprId().equals(slot.getExprId())) {
                slotInChild = expr;
                break;
            }
        }
        if (slotInChild == null) {
            return false;
        } else {
            Set<Slot> slotSet = new HashSet<>();
            if (slotInChild instanceof Alias) {
                slotSet = slotInChild.getInputSlots();
            } else {
                slotSet.add((Slot) slotInChild);
            }
            // get table list from slotSet
            Set<TableIf> tableSets = getTableSets(slotSet, childPlan);
            if (tableSets.isEmpty()) {
                return false;
            } else if (childTables.containsAll(tableSets)) {
                return true;
            } else {
                return false;
            }
        }
    }

    private Set<TableIf> getTableSets(Set<Slot> slotSet, LogicalPlan plan) {
        Set<LogicalCatalogRelation> tableSets = PlanUtils.getLogicalScanFromRootPlan(plan);
        Set<TableIf> resultSet = new HashSet<>();
        for (Slot slot : slotSet) {
            for (LogicalCatalogRelation table : tableSets) {
                if (table.getOutputExprIds().contains(slot.getExprId())) {
                    resultSet.add(table.getTable());
                }
            }
        }
        return resultSet;
    }
}
