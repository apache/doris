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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Pair;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * add constraint command
 */
public class AddConstraintCommand extends Command implements ForwardWithSync {

    private final String name;
    private final Constraint constraint;

    /**
     * constructor
     */
    public AddConstraintCommand(String name, Constraint constraint) {
        super(PlanType.ADD_CONSTRAINT_COMMAND);
        this.constraint = constraint;
        this.name = name;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        Pair<ImmutableList<String>, TableIf> columnsAndTable = extractColumnsAndTable(ctx, constraint.toProject());
        TableIf table = columnsAndTable.second;
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromCatalogDb(
                table.getDatabase().getCatalog(), table.getDatabase(), table);
        ImmutableList<String> columns = columnsAndTable.first;

        Pair<ImmutableList<String>, TableNameInfo> referencedColumnsAndTable = null;
        if (constraint.isForeignKey()) {
            Pair<ImmutableList<String>, TableIf> refColumnsAndTable =
                    extractColumnsAndTable(ctx, constraint.toReferenceProject());
            TableIf refTable = refColumnsAndTable.second;
            TableNameInfo refTableInfo = TableNameInfoUtils.fromCatalogDb(
                    refTable.getDatabase().getCatalog(), refTable.getDatabase(), refTable);
            referencedColumnsAndTable = Pair.of(refColumnsAndTable.first, refTableInfo);
        }
        if (constraint.isForeignKey()) {
            Preconditions.checkState(referencedColumnsAndTable != null);
            addConstraintAndInvalidate(tableNameInfo,
                    new ForeignKeyConstraint(name, columns,
                            referencedColumnsAndTable.second, referencedColumnsAndTable.first));
        } else if (constraint.isPrimaryKey()) {
            addConstraintAndInvalidate(
                    tableNameInfo, new PrimaryKeyConstraint(name, ImmutableSet.copyOf(columns)));
        } else if (constraint.isUnique()) {
            addConstraintAndInvalidate(
                    tableNameInfo, new UniqueConstraint(name, ImmutableSet.copyOf(columns)));
        } else {
            throw new AnalysisException("Unsupported constraint type: " + constraint);
        }
    }

    private void addConstraintAndInvalidate(
            TableNameInfo tableNameInfo, org.apache.doris.catalog.constraint.Constraint constraint)
            throws Exception {
        List<MTMV> dependentMtmvs = MTMVUtil.getDependentMtmvsByConstraint(tableNameInfo, constraint);
        Env.getCurrentEnv().getConstraintManager().addConstraint(tableNameInfo, name, constraint, false);
        MTMVUtil.invalidateRewriteCachesBestEffort(dependentMtmvs,
                String.format("after add constraint %s on table %s", constraint.getName(), tableNameInfo));
    }

    private Pair<ImmutableList<String>, TableIf> extractColumnsAndTable(ConnectContext ctx, LogicalPlan plan) {
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        Plan analyzedPlan = planner.planWithLock(
                plan, PhysicalProperties.ANY, ExplainLevel.ANALYZED_PLAN);
        Set<LogicalCatalogRelation> logicalCatalogRelationSet = analyzedPlan
                .collect(LogicalCatalogRelation.class::isInstance);
        if (logicalCatalogRelationSet.size() != 1) {
            throw new AnalysisException("Can not found table in constraint " + constraint.toString());
        }
        LogicalCatalogRelation catalogRelation = logicalCatalogRelationSet.iterator().next();
        ImmutableList<String> columns = analyzedPlan.getOutput().stream()
                .map(s -> {
                    Preconditions.checkArgument(s instanceof SlotReference
                                    && ((SlotReference) s).getOriginalColumn().isPresent(),
                            "Constraint contains a invalid slot ", s);
                    return ((SlotReference) s).getOriginalColumn().get().getName();
                }).collect(ImmutableList.toImmutableList());
        return Pair.of(columns, catalogRelation.getTable());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAddConstraintCommand(this, context);
    }
}
