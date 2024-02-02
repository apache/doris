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

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


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
        List<NamedExpression> exprList = ((LogicalProject) childPlan).getProjects();
        for (NamedExpression expr : exprList) {
            if (expr.getExprId().equals(slot.getExprId())) {
                slotInChild = expr;
                break;
            }
        }
        if (slotInChild != null) {
            Set<Slot> slotSet = new HashSet<>();
            if (slotInChild instanceof Alias) {
                slotSet = ((Alias) slotInChild).getInputSlots();
            } else {
                slotSet.add((SlotReference) slotInChild);
            }
            // get table list from slotSet
            Set<TableIf> tableSets = getTableIds(slotSet, childPlan);
            if (childTables.containsAll(tableSets)) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private Set<TableIf> getTableIds(Set<Slot> slotSet, LogicalPlan project) {
        List<LogicalCatalogRelation> tableList = getTableListUnderProject(project);
        Set<TableIf> resultSet = new HashSet<>();
        for (Slot slot : slotSet) {
            for (LogicalCatalogRelation table : tableList) {
                if (table.getOutputExprIds().contains(slot.getExprId())) {
                    resultSet.add(table.getTable());
                }
            }
        }
        return resultSet;
    }

    private List<LogicalCatalogRelation> getTableListUnderProject(LogicalPlan project) {
        List<LogicalCatalogRelation> tableLists = new ArrayList<>();
        tableLists.addAll((Collection<? extends LogicalCatalogRelation>) project
                .collect(LogicalCatalogRelation.class::isInstance));
        return tableLists;
    }
}