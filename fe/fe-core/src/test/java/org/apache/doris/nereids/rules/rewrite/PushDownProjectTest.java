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
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference.ArrayItemSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.PreferPushDownProject;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayConcat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayFirst;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
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
    private final ConnectContext connectContext = MemoTestUtils.createConnectContext();

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

    @Test
    public void shouldRewritePreferPushDownProjectInOrFilterToSlot() {
        LogicalPlan rStudent = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student,
                ImmutableList.of(""));
        LogicalPlan rScore = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.score,
                ImmutableList.of(""));
        Expression preferPushDownProjectExpr = new MatchAny(
                new Add(rStudent.getOutput().get(0), Literal.of(1)),
                Literal.of("abc"));
        Expression rightSidePredicate = new GreaterThan(rScore.getOutput().get(2), Literal.of(60));
        Expression orPredicate = new Or(preferPushDownProjectExpr, rightSidePredicate);

        LogicalPlan plan = new LogicalPlanBuilder(rStudent)
                .joinEmptyOn(rScore, JoinType.INNER_JOIN)
                .filter(orPredicate)
                .build();

        PlanChecker.from(connectContext, plan)
                .applyTopDown(new PushDownProject())
                .matchesFromRoot(logicalFilter(
                        logicalJoin(
                                logicalProject(logicalOlapScan())
                                        .when(project -> project.getProjects().stream()
                                                .filter(Alias.class::isInstance)
                                                .map(Alias.class::cast)
                                                .map(Alias::child)
                                                .anyMatch(PreferPushDownProject.class::isInstance)),
                                logicalOlapScan()))
                        .when(filter -> {
                            Expression rewrittenPredicate = ImmutableList.copyOf(filter.getConjuncts()).get(0);
                            return rewrittenPredicate instanceof Or
                                    && rewrittenPredicate.anyMatch(SlotReference.class::isInstance)
                                    && !rewrittenPredicate.anyMatch(PreferPushDownProject.class::isInstance);
                        }));
    }

    @Test
    public void testHasFreeLambdaArgument() {
        StructType pairStructType = new StructType(ImmutableList.of(
                new StructField("col1", IntegerType.INSTANCE, true, ""),
                new StructField("col2", IntegerType.INSTANCE, true, "")
        ));
        ArrayType arrayOfPairsType = ArrayType.of(pairStructType);
        StructType nestedStructType = new StructType(ImmutableList.of(
                new StructField("col1", IntegerType.INSTANCE, true, ""),
                new StructField("nested_arr", arrayOfPairsType, true, "")
        ));
        SlotReference arrSlot = new SlotReference(
                new ExprId(20), "arr", ArrayType.of(nestedStructType), true, ImmutableList.of());
        SlotReference otherArrSlot = new SlotReference(
                new ExprId(21), "arr2", arrayOfPairsType, true, ImmutableList.of());

        ArrayItemReference outerRef = new ArrayItemReference(new ExprId(30), "p", arrSlot);
        ArrayItemSlot outerSlot = (ArrayItemSlot) outerRef.toSlot();
        ElementAt freeElementAt = new ElementAt(outerSlot, Literal.of(1));

        // 1. Sub-expression referencing an unbound ArrayItemSlot has a free lambda argument.
        Assertions.assertTrue(PushDownProject.hasFreeLambdaArgument(freeElementAt));

        // 2. Self-contained ArrayMap binding 'p' has no free lambda arguments.
        Lambda selfContainedLambda = new Lambda(
                ImmutableList.of("p"), freeElementAt, ImmutableList.of(outerRef));
        ArrayMap selfContainedArrayMap = new ArrayMap(selfContainedLambda);
        Assertions.assertFalse(PushDownProject.hasFreeLambdaArgument(selfContainedArrayMap));

        // 3. Inner ArrayMap capturing outer 'p' in its body has a free lambda argument when inspected alone,
        //    but is bound when wrapped by the outer Lambda.
        ArrayItemReference innerRef = new ArrayItemReference(new ExprId(31), "x", otherArrSlot);
        ArrayItemSlot innerSlot = (ArrayItemSlot) innerRef.toSlot();
        Expression innerCapturingBody = new Add(
                freeElementAt, new ElementAt(innerSlot, Literal.of(1)));
        ArrayMap innerCapturingArrayMap = new ArrayMap(new Lambda(
                ImmutableList.of("x"), innerCapturingBody, ImmutableList.of(innerRef)));
        Assertions.assertTrue(PushDownProject.hasFreeLambdaArgument(innerCapturingArrayMap));
        ArrayMap outerWrappingCapturingInner = new ArrayMap(new Lambda(
                ImmutableList.of("p"), innerCapturingArrayMap, ImmutableList.of(outerRef)));
        Assertions.assertFalse(PushDownProject.hasFreeLambdaArgument(outerWrappingCapturingInner));

        // 4. Inner Lambda with a shadowed parameter name ("p") but distinct ExprId does NOT bind outer 'p'.
        ArrayItemReference shadowedInnerRef = new ArrayItemReference(new ExprId(32), "p", otherArrSlot);
        ArrayMap shadowedInnerArrayMap = new ArrayMap(new Lambda(
                ImmutableList.of("p"), freeElementAt, ImmutableList.of(shadowedInnerRef)));
        Assertions.assertTrue(PushDownProject.hasFreeLambdaArgument(shadowedInnerArrayMap));

        // 5. Inner Lambda whose argument arrayExpression references outer 'p' has a free lambda argument
        //    even if its body only uses its own parameter.
        ArrayItemReference innerRefOverOuterSlot = new ArrayItemReference(
                new ExprId(33), "x", new ElementAt(outerSlot, Literal.of(2)));
        ArrayItemSlot innerSlotOverOuter = (ArrayItemSlot) innerRefOverOuterSlot.toSlot();
        ArrayMap innerArrayMapOverOuterSlot = new ArrayMap(new Lambda(
                ImmutableList.of("x"),
                new ElementAt(innerSlotOverOuter, Literal.of(1)),
                ImmutableList.of(innerRefOverOuterSlot)));
        Assertions.assertTrue(PushDownProject.hasFreeLambdaArgument(innerArrayMapOverOuterSlot));
        ArrayMap outerWrappingArrayArgCapture = new ArrayMap(new Lambda(
                ImmutableList.of("p"), innerArrayMapOverOuterSlot, ImmutableList.of(outerRef)));
        Assertions.assertFalse(PushDownProject.hasFreeLambdaArgument(outerWrappingArrayArgCapture));

        // 6. Inner ArrayMap that only references its own bound parameter has no free lambda arguments.
        ArrayMap independentInnerArrayMap = new ArrayMap(new Lambda(
                ImmutableList.of("x"),
                new ElementAt(innerSlot, Literal.of(1)),
                ImmutableList.of(innerRef)));
        Assertions.assertFalse(PushDownProject.hasFreeLambdaArgument(independentInnerArrayMap));

        // 7. Deeply nested lambda chain with scoped binding add/remove:
        //    - closed chain has no free lambda arguments;
        //    - sub-chain whose leaf still references outer 'p' has a free lambda argument until wrapped by outer 'p'.
        Expression nestedClosedChain = new ElementAt(innerSlot, Literal.of(1));
        Expression nestedCapturingChain = new Add(
                freeElementAt, new ElementAt(innerSlot, Literal.of(1)));
        for (int depth = 0; depth < 32; depth++) {
            ArrayItemReference levelRef = new ArrayItemReference(
                    new ExprId(100 + depth), "v" + depth, otherArrSlot);
            Expression levelElem = new ElementAt(levelRef.toSlot(), Literal.of(1));
            nestedClosedChain = new ElementAt(
                    new ArrayMap(new Lambda(
                            ImmutableList.of("v" + depth),
                            new Add(levelElem, nestedClosedChain),
                            ImmutableList.of(levelRef))),
                    Literal.of(1));
            nestedCapturingChain = new ElementAt(
                    new ArrayMap(new Lambda(
                            ImmutableList.of("v" + depth),
                            new Add(levelElem, nestedCapturingChain),
                            ImmutableList.of(levelRef))),
                    Literal.of(1));
        }
        Assertions.assertFalse(PushDownProject.hasFreeLambdaArgument(
                new ArrayMap(new Lambda(ImmutableList.of("x"), nestedClosedChain, ImmutableList.of(innerRef)))));
        ArrayMap wrappedButMissingOuterP = new ArrayMap(
                new Lambda(ImmutableList.of("x"), nestedCapturingChain, ImmutableList.of(innerRef)));
        Assertions.assertTrue(PushDownProject.hasFreeLambdaArgument(wrappedButMissingOuterP));
        Assertions.assertFalse(PushDownProject.hasFreeLambdaArgument(
                new ArrayMap(new Lambda(ImmutableList.of("p"), wrappedButMissingOuterP, ImmutableList.of(outerRef)))));

        // 8. Multi-argument Lambda: (x, y) -> element_at(x, 1) + element_at(y, 1)
        ArrayItemReference secondArgRef = new ArrayItemReference(new ExprId(34), "y", otherArrSlot);
        ArrayItemSlot secondArgSlot = (ArrayItemSlot) secondArgRef.toSlot();
        Expression twoArgBody = new Add(
                new ElementAt(innerSlot, Literal.of(1)),
                new ElementAt(secondArgSlot, Literal.of(1)));
        ArrayMap selfContainedTwoArgMap = new ArrayMap(new Lambda(
                ImmutableList.of("x", "y"), twoArgBody, ImmutableList.of(innerRef, secondArgRef)));
        Assertions.assertFalse(PushDownProject.hasFreeLambdaArgument(selfContainedTwoArgMap));

        ArrayMap capturingTwoArgMap = new ArrayMap(new Lambda(
                ImmutableList.of("x", "y"),
                new Add(twoArgBody, freeElementAt),
                ImmutableList.of(innerRef, secondArgRef)));
        Assertions.assertTrue(PushDownProject.hasFreeLambdaArgument(capturingTwoArgMap));
        Assertions.assertFalse(PushDownProject.hasFreeLambdaArgument(
                new ArrayMap(new Lambda(ImmutableList.of("p"), capturingTwoArgMap, ImmutableList.of(outerRef)))));
    }

    @Test
    public void shouldNotPushDownExpressionWithFreeLambdaArgumentInProjectAboveJoin() {
        StructType pairStructType = new StructType(ImmutableList.of(
                new StructField("col1", IntegerType.INSTANCE, true, ""),
                new StructField("col2", IntegerType.INSTANCE, true, "")
        ));
        SlotReference kSlot = new SlotReference(new ExprId(1), "k", IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference arrSlot = new SlotReference(
                new ExprId(2), "arr", ArrayType.of(pairStructType), true, ImmutableList.of());

        LogicalOneRowRelation leftRel = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(kSlot));
        LogicalOneRowRelation rightRel = new LogicalOneRowRelation(new RelationId(2), ImmutableList.of(arrSlot));

        ArrayItemReference lambdaArg = new ArrayItemReference(new ExprId(3), "p", arrSlot);
        ElementAt structFieldReadOnLambdaArg = new ElementAt(lambdaArg.toSlot(), Literal.of(1));
        Lambda predicateLambda = new Lambda(
                ImmutableList.of("p"),
                new EqualTo(structFieldReadOnLambdaArg, kSlot),
                ImmutableList.of(lambdaArg));
        Expression arrayFirstExpr = new ArrayFirst(predicateLambda).rewriteWhenAnalyze();
        NamedExpression topProjectAlias = new Alias(
                new ExprId(4), new ElementAt(arrayFirstExpr, Literal.of(2)), "v");

        LogicalPlan plan = new LogicalPlanBuilder(leftRel)
                .joinEmptyOn(rightRel, JoinType.CROSS_JOIN)
                .projectExprs(ImmutableList.of(kSlot, topProjectAlias))
                .build();

        PlanChecker.from(connectContext, plan)
                .applyTopDown(new PushDownProject())
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalOneRowRelation(),
                                        logicalOneRowRelation()
                                )
                        ).when(project -> project.getProjects().get(1).anyMatch(
                                e -> e instanceof ElementAt && ((ElementAt) e).child(0) instanceof ArrayItemSlot))
                );
    }

    @Test
    public void shouldPushDownSelfContainedLambdaWhileKeepingFreeLambdaArgumentExpressionInPlace() {
        StructType pairStructType = new StructType(ImmutableList.of(
                new StructField("col1", IntegerType.INSTANCE, true, ""),
                new StructField("col2", IntegerType.INSTANCE, true, "")
        ));
        SlotReference kSlot = new SlotReference(new ExprId(1), "k", IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference arrSlot = new SlotReference(
                new ExprId(2), "arr", ArrayType.of(pairStructType), true, ImmutableList.of());

        LogicalOneRowRelation leftRel = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(kSlot));
        LogicalOneRowRelation rightRel = new LogicalOneRowRelation(new RelationId(2), ImmutableList.of(arrSlot));

        // Cross-side lambda: p -> element_at(p, 1) = k (must stay above join; element_at(p, 1) must not be extracted)
        ArrayItemReference crossRef = new ArrayItemReference(new ExprId(3), "p", arrSlot);
        ArrayMap crossSideArrayMap = new ArrayMap(new Lambda(
                ImmutableList.of("p"),
                new EqualTo(new ElementAt(crossRef.toSlot(), Literal.of(1)), kSlot),
                ImmutableList.of(crossRef)));
        NamedExpression crossSideAlias = new Alias(new ExprId(4), crossSideArrayMap, "cross_map");

        // Self-contained right-side lambda: q -> element_at(q, 2) (should be pushed down to right join child)
        ArrayItemReference rightOnlyRef = new ArrayItemReference(new ExprId(5), "q", arrSlot);
        ArrayMap rightOnlyArrayMap = new ArrayMap(new Lambda(
                ImmutableList.of("q"),
                new ElementAt(rightOnlyRef.toSlot(), Literal.of(2)),
                ImmutableList.of(rightOnlyRef)));
        NamedExpression rightOnlyAlias = new Alias(new ExprId(6), rightOnlyArrayMap, "right_map");

        LogicalPlan plan = new LogicalPlanBuilder(leftRel)
                .joinEmptyOn(rightRel, JoinType.INNER_JOIN)
                .projectExprs(ImmutableList.of(kSlot, crossSideAlias, rightOnlyAlias))
                .build();

        PlanChecker.from(connectContext, plan)
                .applyTopDown(new PushDownProject())
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalOneRowRelation(),
                                        logicalProject(logicalOneRowRelation())
                                                .when(rightProj -> rightProj.getProjects().size() == 2
                                                        && rightProj.getProjects().get(1) instanceof Alias
                                                        && rightProj.getProjects().get(1).child(0) instanceof ArrayMap)
                                )
                        ).when(topProj -> topProj.getProjects().get(1).child(0) instanceof ArrayMap
                                && topProj.getProjects().get(1).anyMatch(
                                        e -> e instanceof ElementAt
                                                && ((ElementAt) e).child(0) instanceof ArrayItemSlot)
                                && topProj.getProjects().get(2).child(0) instanceof SlotReference)
                );
    }

    @Test
    public void shouldNotPushDownInnerLambdaCapturingOuterLambdaArgument() {
        StructType pairStructType = new StructType(ImmutableList.of(
                new StructField("col1", IntegerType.INSTANCE, true, ""),
                new StructField("col2", IntegerType.INSTANCE, true, "")
        ));
        SlotReference leftArrSlot = new SlotReference(
                new ExprId(1), "left_arr", ArrayType.of(pairStructType), true, ImmutableList.of());
        SlotReference rightArrSlot = new SlotReference(
                new ExprId(2), "right_arr", ArrayType.of(pairStructType), true, ImmutableList.of());
        SlotReference rightNestedArrSlot = new SlotReference(
                new ExprId(9), "right_nested_arr", ArrayType.of(ArrayType.of(pairStructType)),
                true, ImmutableList.of());

        LogicalOneRowRelation leftRel = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(leftArrSlot));
        LogicalOneRowRelation rightRel = new LogicalOneRowRelation(
                new RelationId(2), ImmutableList.of(rightArrSlot, rightNestedArrSlot));

        // Outer lambda iterates right_arr (from right child); inner lambda iterates left_arr (from left child)
        // and captures outer parameter 'p'. Neither the inner ArrayMap nor element_at(p, 1) may be pushed down.
        ArrayItemReference outerRef = new ArrayItemReference(new ExprId(3), "p", rightArrSlot);
        ArrayItemReference innerRef = new ArrayItemReference(new ExprId(4), "x", leftArrSlot);
        ArrayMap capturingInnerArrayMap = new ArrayMap(new Lambda(
                ImmutableList.of("x"),
                new Add(new ElementAt(outerRef.toSlot(), Literal.of(1)),
                        new ElementAt(innerRef.toSlot(), Literal.of(1))),
                ImmutableList.of(innerRef)));
        ArrayMap outerArrayMap = new ArrayMap(new Lambda(
                ImmutableList.of("p"),
                capturingInnerArrayMap,
                ImmutableList.of(outerRef)));

        // A second nested expression where the inner ArrayMap over left_arr does NOT capture outer 'q':
        // the inner PreferPushDownProject expression CAN be pushed down to leftRel while element_at(q, 1)
        // stays inside the outer lambda.
        ArrayItemReference outerRef2 = new ArrayItemReference(new ExprId(5), "q", rightArrSlot);
        ArrayItemReference independentInnerRef = new ArrayItemReference(new ExprId(6), "y", leftArrSlot);
        ArrayMap independentInnerArrayMap = new ArrayMap(new Lambda(
                ImmutableList.of("y"),
                new ElementAt(independentInnerRef.toSlot(), Literal.of(1)),
                ImmutableList.of(independentInnerRef)));
        ElementAt independentLeftElementAt = new ElementAt(independentInnerArrayMap, Literal.of(1));
        ArrayMap outerArrayMapWithIndependentInner = new ArrayMap(new Lambda(
                ImmutableList.of("q"),
                new Add(new ElementAt(outerRef2.toSlot(), Literal.of(1)), independentLeftElementAt),
                ImmutableList.of(outerRef2)));

        // A third expression with a deep nested chain F_i = array_map(x_i -> x_i, F_(i+1)) over outer 'r'
        // combined via array_concat(F_1, left_arr): every F_i has empty getInputSlots() and must be rejected
        // in a single linear pass without pushing any F_i into leftRel or rightRel.
        ArrayItemReference chainOuterRef = new ArrayItemReference(new ExprId(10), "r", rightNestedArrSlot);
        Expression nestedChain = chainOuterRef.toSlot();
        for (int i = 0; i < 64; i++) {
            ArrayItemReference stepRef = new ArrayItemReference(
                    new ExprId(200 + i), "x" + i, nestedChain);
            nestedChain = new ArrayMap(new Lambda(
                    ImmutableList.of("x" + i),
                    stepRef.toSlot(),
                    ImmutableList.of(stepRef)));
        }
        ArrayMap outerChainArrayMap = new ArrayMap(new Lambda(
                ImmutableList.of("r"),
                new ArrayConcat(nestedChain, leftArrSlot),
                ImmutableList.of(chainOuterRef)));

        LogicalPlan plan = new LogicalPlanBuilder(leftRel)
                .joinEmptyOn(rightRel, JoinType.INNER_JOIN)
                .projectExprs(ImmutableList.<NamedExpression>of(
                        new Alias(new ExprId(7), outerArrayMap, "captured"),
                        new Alias(new ExprId(8), outerArrayMapWithIndependentInner, "independent_inner"),
                        new Alias(new ExprId(11), outerChainArrayMap, "deep_chain")))
                .build();

        PlanChecker.from(connectContext, plan)
                .applyTopDown(new PushDownProject())
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(logicalOneRowRelation())
                                                .when(leftProj -> leftProj.getProjects().size() == 2
                                                        && leftProj.getProjects().get(1).child(0)
                                                                .equals(independentLeftElementAt)),
                                        logicalOneRowRelation()
                                )
                        ).when(topProj -> topProj.getProjects().get(0).child(0).equals(outerArrayMap)
                                && topProj.getProjects().get(1).anyMatch(
                                        e -> e instanceof ElementAt
                                                && ((ElementAt) e).child(0).equals(outerRef2.toSlot()))
                                && topProj.getProjects().get(2).child(0).equals(outerChainArrayMap))
                );
    }

    @Test
    public void shouldNotPushDownFreeLambdaArgumentInJoinConjunctsFilterAndUnion() {
        StructType pairStructType = new StructType(ImmutableList.of(
                new StructField("col1", IntegerType.INSTANCE, true, ""),
                new StructField("col2", IntegerType.INSTANCE, true, "")
        ));
        SlotReference kSlot = new SlotReference(new ExprId(1), "k", IntegerType.INSTANCE, true, ImmutableList.of());
        SlotReference arrSlot = new SlotReference(
                new ExprId(2), "arr", ArrayType.of(pairStructType), true, ImmutableList.of());

        LogicalOneRowRelation leftRel = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(kSlot));
        LogicalOneRowRelation rightRel = new LogicalOneRowRelation(new RelationId(2), ImmutableList.of(arrSlot));

        ArrayItemReference lambdaArg = new ArrayItemReference(new ExprId(3), "p", arrSlot);
        Expression crossSidePredicate = new EqualTo(
                new ElementAt(
                        new ArrayMap(new Lambda(
                                ImmutableList.of("p"),
                                new EqualTo(new ElementAt(lambdaArg.toSlot(), Literal.of(1)), kSlot),
                                ImmutableList.of(lambdaArg))),
                        Literal.of(1)),
                Literal.of(true));

        // 1. Join otherConjuncts path (pushDownJoinExpressions -> pushDownProjectInExpressions)
        LogicalPlan joinWithConjunctPlan = new LogicalPlanBuilder(leftRel)
                .join(rightRel, JoinType.INNER_JOIN, ImmutableList.of(), ImmutableList.of(crossSidePredicate))
                .build();
        PlanChecker.from(connectContext, joinWithConjunctPlan)
                .applyTopDown(new PushDownProject())
                .matchesFromRoot(
                        logicalJoin(
                                logicalOneRowRelation(),
                                logicalOneRowRelation()
                        ).when(join -> join.getOtherJoinConjuncts().get(0).equals(crossSidePredicate))
                );

        // 2. Filter above join path (pushDownFilterExpressions -> PushdownProjectHelper)
        LogicalPlan filterAboveJoinPlan = new LogicalPlanBuilder(leftRel)
                .joinEmptyOn(rightRel, JoinType.INNER_JOIN)
                .filter(crossSidePredicate)
                .build();
        PlanChecker.from(connectContext, filterAboveJoinPlan)
                .applyTopDown(new PushDownProject())
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin(
                                        logicalOneRowRelation(),
                                        logicalOneRowRelation()
                                )
                        ).when(filter -> filter.getConjuncts().contains(crossSidePredicate))
                );

        // 3. Union path (pushThroughUnion -> PushdownProjectHelper) with an unbound lambda slot expression
        ArrayItemReference unionLambdaArg = new ArrayItemReference(new ExprId(99), "u", arrSlot);
        NamedExpression freeLambdaAliasOnUnion = new Alias(
                new ExprId(105), new ElementAt(unionLambdaArg.toSlot(), Literal.of(1)), "free_elem");
        LogicalUnion union = new LogicalUnion(
                Qualifier.ALL, unionOutput,
                Lists.newArrayList(regulatorRel1Output, regulatorRel2Output),
                Lists.newArrayList(), true, children);
        LogicalProject<LogicalUnion> unionProject = new LogicalProject<>(
                ImmutableList.of(freeLambdaAliasOnUnion), union);
        Plan resUnionPlan = PushDownProject.pushThroughUnion(unionProject, new StatementContext());
        Assertions.assertSame(unionProject, resUnionPlan);

        // 4. Union with both regular children and constantExprsList: pushable ElementAt is pushed into
        //    both regular children and constant one-row branches while freeLambdaAliasOnUnion stays in place.
        LogicalUnion unionWithConstants = new LogicalUnion(
                Qualifier.ALL, unionOutput,
                Lists.newArrayList(regulatorRel1Output),
                Lists.newArrayList(regulatorRel2Output),
                true, Lists.newArrayList(rel1));
        LogicalProject<LogicalUnion> mixedUnionProject = new LogicalProject<>(
                ImmutableList.of(freeLambdaAliasOnUnion, pushDownProjections.get(0)), unionWithConstants);
        LogicalProject<LogicalUnion> resMixedUnion = (LogicalProject<LogicalUnion>) PushDownProject.pushThroughUnion(
                mixedUnionProject, new StatementContext());
        Assertions.assertEquals(freeLambdaAliasOnUnion, resMixedUnion.getProjects().get(0));
        Assertions.assertFalse(resMixedUnion.getProjects().get(1).containsType(ElementAt.class));
        Assertions.assertEquals(2, resMixedUnion.child().arity());
    }
}
