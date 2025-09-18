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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.implementation.LogicalOlapTableSinkToPhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.concurrent.atomic.AtomicReference;

/** FastInsertIntoValuesPlanner */
public class FastInsertIntoValuesPlanner extends NereidsPlanner {
    private static final Rule toPhysicalOlapTableSink = new LogicalOlapTableSinkToPhysicalOlapTableSink()
            .build();
    protected final boolean fastInsertIntoValues;
    protected final boolean batchInsert;
    private final AtomicReference<Group> rootGroupRef = new AtomicReference<>();

    public FastInsertIntoValuesPlanner(StatementContext statementContext, boolean fastInsertIntoValues) {
        this(statementContext, fastInsertIntoValues, false);
    }

    public FastInsertIntoValuesPlanner(
            StatementContext statementContext, boolean fastInsertIntoValues, boolean batchInsert) {
        super(statementContext);
        this.fastInsertIntoValues = fastInsertIntoValues;
        this.batchInsert = batchInsert;
    }

    @Override
    protected void analyze(boolean showPlanProcess) {
        if (!fastInsertIntoValues) {
            super.analyze(showPlanProcess);
            return;
        }
        CascadesContext cascadesContext = getCascadesContext();
        keepOrShowPlanProcess(showPlanProcess, () -> {
            InsertIntoValuesAnalyzer analyzer = new InsertIntoValuesAnalyzer(cascadesContext, batchInsert);
            analyzer.execute();
        });
    }

    @Override
    protected void rewrite(boolean showPlanProcess) {
        if (!fastInsertIntoValues) {
            super.rewrite(showPlanProcess);
        }
    }

    @Override
    protected void optimize(boolean showPlanProcess) {
        if (!fastInsertIntoValues) {
            super.optimize(showPlanProcess);
            return;
        }

        DefaultPlanRewriter<Void> optimizer = new DefaultPlanRewriter<Void>() {
            @Override
            public Plan visitLogicalUnion(LogicalUnion logicalUnion, Void context) {
                logicalUnion = (LogicalUnion) super.visitLogicalUnion(logicalUnion, context);

                return new PhysicalUnion(logicalUnion.getQualifier(),
                        logicalUnion.getOutputs(),
                        logicalUnion.getRegularChildrenOutputs(),
                        logicalUnion.getConstantExprsList(),
                        logicalUnion.getLogicalProperties(),
                        logicalUnion.children()
                );
            }

            @Override
            public Plan visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, Void context) {
                return new PhysicalOneRowRelation(
                        oneRowRelation.getRelationId(),
                        oneRowRelation.getProjects(),
                        oneRowRelation.getLogicalProperties());
            }

            @Override
            public Plan visitLogicalProject(LogicalProject<? extends Plan> logicalProject, Void context) {
                logicalProject =
                        (LogicalProject<? extends Plan>) super.visitLogicalProject(logicalProject, context);

                return new PhysicalProject<>(
                        logicalProject.getProjects(),
                        logicalProject.getLogicalProperties(),
                        logicalProject.child()
                );
            }

            @Override
            public Plan visitLogicalOlapTableSink(LogicalOlapTableSink<? extends Plan> olapTableSink,
                    Void context) {
                olapTableSink =
                        (LogicalOlapTableSink) super.visitLogicalOlapTableSink(olapTableSink, context);
                return toPhysicalOlapTableSink
                        .transform(olapTableSink, getCascadesContext())
                        .get(0);
            }
        };

        PhysicalPlan physicalPlan =
                (PhysicalPlan) getCascadesContext().getRewritePlan().accept(optimizer, null);

        super.physicalPlan = physicalPlan;

        GroupId rootGroupId = GroupId.createGenerator().getNextId();
        Group rootGroup = new Group(rootGroupId, physicalPlan.getLogicalProperties());
        rootGroupRef.set(rootGroup);
    }

    @Override
    public Group getRoot() {
        if (!fastInsertIntoValues) {
            return super.getRoot();
        }
        return rootGroupRef.get();
    }

    @Override
    protected PhysicalPlan chooseNthPlan(
            Group rootGroup, PhysicalProperties physicalProperties, int nthPlan) {
        if (!fastInsertIntoValues) {
            return super.chooseNthPlan(rootGroup, physicalProperties, nthPlan);
        }
        return super.physicalPlan;
    }

    @Override
    protected PhysicalPlan postProcess(PhysicalPlan physicalPlan) {
        if (!fastInsertIntoValues) {
            return super.postProcess(physicalPlan);
        }
        return physicalPlan;
    }
}
