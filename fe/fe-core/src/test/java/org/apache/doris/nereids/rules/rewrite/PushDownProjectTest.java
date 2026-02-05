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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PushDownProjectTest implements MemoPatternMatchSupported {

    private final List rel1Output = Lists.newArrayList(
            new SlotReference(new ExprId(1), "c1", TinyIntType.INSTANCE, true, Lists.newArrayList()),
            new SlotReference(new ExprId(2), "c2", TinyIntType.INSTANCE, true, Lists.newArrayList())
    );
    private final List regulatorRel1Output = Lists.newArrayList(
            new SlotReference(new ExprId(2), "c2", TinyIntType.INSTANCE, true, Lists.newArrayList()),
            new SlotReference(new ExprId(1), "c1", TinyIntType.INSTANCE, true, Lists.newArrayList()),
            new SlotReference(new ExprId(2), "c2", TinyIntType.INSTANCE, true, Lists.newArrayList())
    );
    private final List rel2Output = Lists.newArrayList(
            new SlotReference(new ExprId(3), "c3", TinyIntType.INSTANCE, true, Lists.newArrayList()),
            new SlotReference(new ExprId(4), "c4", TinyIntType.INSTANCE, true, Lists.newArrayList()),
            new SlotReference(new ExprId(5), "c5", TinyIntType.INSTANCE, true, Lists.newArrayList())
    );
    private final List regulatorRel2Output = Lists.newArrayList(
            new SlotReference(new ExprId(3), "c3", TinyIntType.INSTANCE, true, Lists.newArrayList()),
            new SlotReference(new ExprId(5), "c5", TinyIntType.INSTANCE, true, Lists.newArrayList()),
            new SlotReference(new ExprId(4), "c4", TinyIntType.INSTANCE, true, Lists.newArrayList())
    );
    private final List<NamedExpression> unionOutput = Lists.newArrayList(
            new SlotReference(new ExprId(10), "c10", TinyIntType.INSTANCE, true, Lists.newArrayList()),
            new SlotReference(new ExprId(11), "c11", TinyIntType.INSTANCE, true, Lists.newArrayList()),
            new SlotReference(new ExprId(12), "c12", TinyIntType.INSTANCE, true, Lists.newArrayList())
    );
    private final List<NamedExpression> pushDownProjections = Lists.newArrayList(
            new Alias(new ExprId(100), new ElementAt(
                    new SlotReference(new ExprId(10), "c10", TinyIntType.INSTANCE, true, Lists.newArrayList()),
                    new StringLiteral("a"))),
            new Alias(new ExprId(101), new ElementAt(
                    new SlotReference(new ExprId(10), "c10", TinyIntType.INSTANCE, true, Lists.newArrayList()),
                    new StringLiteral("b"))),
            new Alias(new ExprId(102), new ElementAt(
                    new SlotReference(new ExprId(12), "c10", TinyIntType.INSTANCE, true, Lists.newArrayList()),
                    new StringLiteral("a"))),
            new SlotReference(new ExprId(11), "c11", TinyIntType.INSTANCE, true, Lists.newArrayList())
    );

    private final LogicalOneRowRelation rel1 = new LogicalOneRowRelation(new RelationId(1), rel1Output);
    private final LogicalOneRowRelation rel2 = new LogicalOneRowRelation(new RelationId(2), rel2Output);
    private final List<Plan> children = Lists.newArrayList(rel1, rel2);

    @Test
    public void testPushDownProjectThroughUnionOnlyHasChildren() {
        List<List<SlotReference>> regulatorOutputs = Lists.newArrayList(regulatorRel1Output, regulatorRel2Output);
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, unionOutput,
                regulatorOutputs, Lists.newArrayList(), true, children);
        LogicalProject<LogicalUnion> project = new LogicalProject<>(pushDownProjections, union);
        StatementContext context = new StatementContext();
        LogicalProject<LogicalUnion> resProject
                = (LogicalProject<LogicalUnion>) PushDownProject.pushThroughUnion(project, context);
        PlanChecker.from(MemoTestUtils.createConnectContext(), resProject)
                .matchesFromRoot(
                        logicalProject(
                                logicalUnion(
                                        logicalProject(
                                                logicalOneRowRelation()
                                                        .when(r -> r.getRelationId().asInt() == 1)
                                                        .when(r -> r.getOutputs().size() == 2)
                                        ).when(p -> p.getOutputs().size() == 6)
                                                .when(p -> p.getProjects().get(0).getExprId().asInt() == 2)
                                                .when(p -> p.getProjects().get(1).getExprId().asInt() == 1)
                                                .when(p -> p.getProjects().get(2).getExprId().asInt() == 2)
                                                .when(p -> p.getProjects().get(3).child(0) instanceof ElementAt)
                                                .when(p -> p.getProjects().get(4).child(0) instanceof ElementAt)
                                                .when(p -> p.getProjects().get(5).child(0) instanceof ElementAt)
                                                .when(p -> ((SlotReference) (p.getProjects().get(3).child(0).child(0))).getExprId().asInt() == 2)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(3).child(0).child(1))).getValue().equals("a"))
                                                .when(p -> ((SlotReference) (p.getProjects().get(4).child(0).child(0))).getExprId().asInt() == 2)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(4).child(0).child(1))).getValue().equals("b"))
                                                .when(p -> ((SlotReference) (p.getProjects().get(5).child(0).child(0))).getExprId().asInt() == 2)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(5).child(0).child(1))).getValue().equals("a")),
                                        logicalProject(
                                                logicalOneRowRelation()
                                                        .when(r -> r.getRelationId().asInt() == 2)
                                                        .when(r -> r.getOutputs().size() == 3)
                                        ).when(p -> p.getOutputs().size() == 6)
                                                .when(p -> p.getProjects().get(0).getExprId().asInt() == 3)
                                                .when(p -> p.getProjects().get(1).getExprId().asInt() == 5)
                                                .when(p -> p.getProjects().get(2).getExprId().asInt() == 4)
                                                .when(p -> p.getProjects().get(3).child(0) instanceof ElementAt)
                                                .when(p -> p.getProjects().get(4).child(0) instanceof ElementAt)
                                                .when(p -> p.getProjects().get(5).child(0) instanceof ElementAt)
                                                .when(p -> ((SlotReference) (p.getProjects().get(3).child(0).child(0))).getExprId().asInt() == 3)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(3).child(0).child(1))).getValue().equals("a"))
                                                .when(p -> ((SlotReference) (p.getProjects().get(4).child(0).child(0))).getExprId().asInt() == 3)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(4).child(0).child(1))).getValue().equals("b"))
                                                .when(p -> ((SlotReference) (p.getProjects().get(5).child(0).child(0))).getExprId().asInt() == 4)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(5).child(0).child(1))).getValue().equals("a"))
                                ).when(u -> u.getOutput().size() == 6)
                                        .when(u -> u.getOutput().get(0).getExprId().asInt() == 10)
                                        .when(u -> u.getOutput().get(1).getExprId().asInt() == 11)
                                        .when(u -> u.getOutput().get(2).getExprId().asInt() == 12)
                        ).when(p -> p.getProjects().stream().noneMatch(ne -> ne.containsType(ElementAt.class)))
                );
    }

    @Test
    public void testPushDownProjectThroughUnionHasNoChildren() {
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, unionOutput, Lists.newArrayList(),
                Lists.newArrayList(regulatorRel1Output, regulatorRel2Output), true, Lists.newArrayList());
        LogicalProject<LogicalUnion> project = new LogicalProject<>(pushDownProjections, union);
        StatementContext context = new StatementContext();
        LogicalProject<LogicalUnion> resProject
                = (LogicalProject<LogicalUnion>) PushDownProject.pushThroughUnion(project, context);
        PlanChecker.from(MemoTestUtils.createConnectContext(), resProject)
                .matchesFromRoot(
                        logicalProject(
                                logicalUnion(
                                        logicalProject(
                                                logicalOneRowRelation()
                                                        .when(r -> r.getOutputs().size() == 3)
                                        ).when(p -> p.getOutputs().size() == 6)
                                                .when(p -> p.getProjects().get(0).getExprId().asInt() == 2)
                                                .when(p -> p.getProjects().get(1).getExprId().asInt() == 1)
                                                .when(p -> p.getProjects().get(2).getExprId().asInt() == 2)
                                                .when(p -> p.getProjects().get(3).child(0) instanceof ElementAt)
                                                .when(p -> p.getProjects().get(4).child(0) instanceof ElementAt)
                                                .when(p -> p.getProjects().get(5).child(0) instanceof ElementAt)
                                                .when(p -> ((SlotReference) (p.getProjects().get(3).child(0).child(0))).getExprId().asInt() == 2)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(3).child(0).child(1))).getValue().equals("a"))
                                                .when(p -> ((SlotReference) (p.getProjects().get(4).child(0).child(0))).getExprId().asInt() == 2)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(4).child(0).child(1))).getValue().equals("b"))
                                                .when(p -> ((SlotReference) (p.getProjects().get(5).child(0).child(0))).getExprId().asInt() == 2)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(5).child(0).child(1))).getValue().equals("a")),
                                        logicalProject(
                                                logicalOneRowRelation()
                                                        .when(r -> r.getOutputs().size() == 3)
                                        ).when(p -> p.getOutputs().size() == 6)
                                                .when(p -> p.getProjects().get(0).getExprId().asInt() == 3)
                                                .when(p -> p.getProjects().get(1).getExprId().asInt() == 5)
                                                .when(p -> p.getProjects().get(2).getExprId().asInt() == 4)
                                                .when(p -> p.getProjects().get(3).child(0) instanceof ElementAt)
                                                .when(p -> p.getProjects().get(4).child(0) instanceof ElementAt)
                                                .when(p -> p.getProjects().get(5).child(0) instanceof ElementAt)
                                                .when(p -> ((SlotReference) (p.getProjects().get(3).child(0).child(0))).getExprId().asInt() == 3)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(3).child(0).child(1))).getValue().equals("a"))
                                                .when(p -> ((SlotReference) (p.getProjects().get(4).child(0).child(0))).getExprId().asInt() == 3)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(4).child(0).child(1))).getValue().equals("b"))
                                                .when(p -> ((SlotReference) (p.getProjects().get(5).child(0).child(0))).getExprId().asInt() == 4)
                                                .when(p -> ((StringLiteral) (p.getProjects().get(5).child(0).child(1))).getValue().equals("a"))
                                ).when(u -> u.getOutput().size() == 6)
                                        .when(u -> u.getOutput().get(0).getExprId().asInt() == 10)
                                        .when(u -> u.getOutput().get(1).getExprId().asInt() == 11)
                                        .when(u -> u.getOutput().get(2).getExprId().asInt() == 12)
                        ).when(p -> p.getProjects().stream().noneMatch(ne -> ne.containsType(ElementAt.class)))
                );
    }
}
