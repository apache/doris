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

package org.apache.doris.nereids.memo;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.FakePlan;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

class MemoTest implements PatternMatchSupported {

    private final ConnectContext connectContext = MemoTestUtils.createConnectContext();

    private final LogicalJoin<LogicalOlapScan, LogicalOlapScan> logicalJoinAB = new LogicalJoin<>(JoinType.INNER_JOIN,
            PlanConstructor.newLogicalOlapScan(0, "A", 0),
            PlanConstructor.newLogicalOlapScan(1, "B", 0));

    private final LogicalJoin<LogicalJoin<LogicalOlapScan, LogicalOlapScan>, LogicalOlapScan> logicalJoinABC = new LogicalJoin<>(
            JoinType.INNER_JOIN, logicalJoinAB, PlanConstructor.newLogicalOlapScan(2, "C", 0));

    @Test
    void mergeGroup() {
        Memo memo = new Memo();
        GroupId gid2 = new GroupId(2);
        Group srcGroup = new Group(gid2, new GroupExpression(new FakePlan()), new LogicalProperties(ArrayList::new));
        GroupId gid3 = new GroupId(3);
        Group dstGroup = new Group(gid3, new GroupExpression(new FakePlan()), new LogicalProperties(ArrayList::new));
        FakePlan d = new FakePlan();
        GroupExpression ge1 = new GroupExpression(d, Arrays.asList(srcGroup));
        GroupId gid0 = new GroupId(0);
        Group g1 = new Group(gid0, ge1, new LogicalProperties(ArrayList::new));
        g1.setBestPlan(ge1, Double.MIN_VALUE, PhysicalProperties.ANY);
        GroupExpression ge2 = new GroupExpression(d, Arrays.asList(dstGroup));
        GroupId gid1 = new GroupId(1);
        Group g2 = new Group(gid1, ge2, new LogicalProperties(ArrayList::new));
        Map<GroupId, Group> groups = Deencapsulation.getField(memo, "groups");
        groups.put(gid2, srcGroup);
        groups.put(gid3, dstGroup);
        groups.put(gid0, g1);
        groups.put(gid1, g2);
        Map<GroupExpression, GroupExpression> groupExpressions =
                Deencapsulation.getField(memo, "groupExpressions");
        groupExpressions.put(ge1, ge1);
        groupExpressions.put(ge2, ge2);
        memo.mergeGroup(srcGroup, dstGroup);
        Assertions.assertNull(g1.getBestPlan(PhysicalProperties.ANY));
        Assertions.assertEquals(ge1.getOwnerGroup(), g2);
    }

    /**
     * Original:
     * Group 0: LogicalOlapScan C
     * Group 1: LogicalOlapScan B
     * Group 2: LogicalOlapScan A
     * Group 3: Join(Group 1, Group 2)
     * Group 4: Join(Group 0, Group 3)
     * <p>
     * Then:
     * Copy In Join(Group 2, Group 1) into Group 3
     * <p>
     * Expected:
     * Group 0: LogicalOlapScan C
     * Group 1: LogicalOlapScan B
     * Group 2: LogicalOlapScan A
     * Group 3: Join(Group 1, Group 2), Join(Group 2, Group 1)
     * Group 4: Join(Group 0, Group 3)
     */
    @Test
    public void testInsertSameGroup() {
        PlanChecker.from(MemoTestUtils.createConnectContext(), logicalJoinABC)
                .transform(
                        // swap join's children
                        logicalJoin(logicalOlapScan(), logicalOlapScan()).then(joinBA ->
                                new LogicalProject<>(Lists.newArrayList(joinBA.getOutput()),
                                        new LogicalJoin<>(JoinType.INNER_JOIN, joinBA.right(), joinBA.left()))
                        ))
                .checkGroupNum(6)
                .checkGroupExpressionNum(7)
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(1, root.getLogicalExpressions().size());
                    GroupExpression joinABC = root.getLogicalExpression();
                    Assertions.assertEquals(2, joinABC.child(0).getLogicalExpressions().size());
                    Assertions.assertEquals(1, joinABC.child(1).getLogicalExpressions().size());
                    GroupExpression joinAB = joinABC.child(0).getLogicalExpressions().get(0);
                    GroupExpression project = joinABC.child(0).getLogicalExpressions().get(1);
                    GroupExpression joinBA = project.child(0).getLogicalExpression();
                    Assertions.assertTrue(joinAB.getPlan() instanceof LogicalJoin);
                    Assertions.assertTrue(joinBA.getPlan() instanceof LogicalJoin);
                });

    }

    @Test
    public void initByOneLevelPlan() {
        OlapTable table = PlanConstructor.newOlapTable(0, "a", 1);
        LogicalOlapScan scan = new LogicalOlapScan(RelationId.createGenerator().getNextId(), table);

        PlanChecker.from(connectContext, scan)
                .checkGroupNum(1)
                .matches(
                        logicalOlapScan().when(scan::equals)
                );
    }

    @Test
    public void initByTwoLevelChainPlan() {
        OlapTable table = PlanConstructor.newOlapTable(0, "a", 1);
        LogicalOlapScan scan = new LogicalOlapScan(RelationId.createGenerator().getNextId(), table);

        LogicalProject<LogicalOlapScan> topProject = new LogicalProject<>(
                ImmutableList.of(scan.computeOutput().get(0)), scan);

        PlanChecker.from(connectContext, topProject)
                .checkGroupNum(2)
                .matches(
                        logicalProject(
                                any().when(child -> Objects.equals(child, scan))
                        ).when(root -> Objects.equals(root, topProject))
                );
    }

    @Test
    public void initByJoinSameUnboundTable() {
        UnboundRelation scanA = new UnboundRelation(ImmutableList.of("a"));

        LogicalJoin<UnboundRelation, UnboundRelation> topJoin = new LogicalJoin<>(JoinType.INNER_JOIN, scanA, scanA);

        PlanChecker.from(connectContext, topJoin)
                .checkGroupNum(3)
                .matches(
                        logicalJoin(
                                any().when(left -> Objects.equals(left, scanA)),
                                any().when(right -> Objects.equals(right, scanA))
                        ).when(root -> Objects.equals(root, topJoin))
                );
    }

    @Test
    public void initByJoinSameLogicalTable() {
        IdGenerator<RelationId> generator = RelationId.createGenerator();
        OlapTable tableA = PlanConstructor.newOlapTable(0, "a", 1);
        LogicalOlapScan scanA = new LogicalOlapScan(generator.getNextId(), tableA);
        LogicalOlapScan scanA1 = new LogicalOlapScan(generator.getNextId(), tableA);

        LogicalJoin<LogicalOlapScan, LogicalOlapScan> topJoin = new LogicalJoin<>(JoinType.INNER_JOIN, scanA, scanA1);

        PlanChecker.from(connectContext, topJoin)
                .checkGroupNum(3)
                .matches(
                        logicalJoin(
                                any().when(left -> Objects.equals(left, scanA)),
                                any().when(right -> Objects.equals(right, scanA1))
                        ).when(root -> Objects.equals(root, topJoin))
                );
    }

    @Test
    public void initByTwoLevelJoinPlan() {
        IdGenerator<RelationId> generator = RelationId.createGenerator();
        OlapTable tableA = PlanConstructor.newOlapTable(0, "a", 1);
        OlapTable tableB = PlanConstructor.newOlapTable(0, "b", 1);
        LogicalOlapScan scanA = new LogicalOlapScan(generator.getNextId(), tableA);
        LogicalOlapScan scanB = new LogicalOlapScan(generator.getNextId(), tableB);

        LogicalJoin<LogicalOlapScan, LogicalOlapScan> topJoin = new LogicalJoin<>(JoinType.INNER_JOIN, scanA, scanB);

        PlanChecker.from(connectContext, topJoin)
                .checkGroupNum(3)
                .matches(
                        logicalJoin(
                                any().when(left -> Objects.equals(left, scanA)),
                                any().when(right -> Objects.equals(right, scanB))
                        ).when(root -> Objects.equals(root, topJoin))
                );
    }

    @Test
    public void initByThreeLevelChainPlan() {
        OlapTable table = PlanConstructor.newOlapTable(0, "a", 1);
        LogicalOlapScan scan = new LogicalOlapScan(RelationId.createGenerator().getNextId(), table);

        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(scan.computeOutput().get(0)), scan);
        LogicalFilter<LogicalProject<LogicalOlapScan>> filter = new LogicalFilter<>(
                new EqualTo(scan.computeOutput().get(0), new IntegerLiteral(1)), project);

        PlanChecker.from(connectContext, filter)
                .checkGroupNum(3)
                .matches(
                        logicalFilter(
                                logicalProject(
                                        any().when(child -> Objects.equals(child, scan))
                                ).when(root -> Objects.equals(root, project))
                        ).when(root -> Objects.equals(root, filter))
                );
    }

    @Test
    public void initByThreeLevelBushyPlan() {
        IdGenerator<RelationId> generator = RelationId.createGenerator();
        OlapTable tableA = PlanConstructor.newOlapTable(0, "a", 1);
        OlapTable tableB = PlanConstructor.newOlapTable(0, "b", 1);
        OlapTable tableC = PlanConstructor.newOlapTable(0, "c", 1);
        OlapTable tableD = PlanConstructor.newOlapTable(0, "d", 1);
        LogicalOlapScan scanA = new LogicalOlapScan(generator.getNextId(), tableA);
        LogicalOlapScan scanB = new LogicalOlapScan(generator.getNextId(), tableB);
        LogicalOlapScan scanC = new LogicalOlapScan(generator.getNextId(), tableC);
        LogicalOlapScan scanD = new LogicalOlapScan(generator.getNextId(), tableD);

        LogicalJoin<LogicalOlapScan, LogicalOlapScan> leftJoin = new LogicalJoin<>(JoinType.CROSS_JOIN, scanA, scanB);
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> rightJoin = new LogicalJoin<>(JoinType.CROSS_JOIN, scanC, scanD);
        LogicalJoin topJoin = new LogicalJoin<>(JoinType.CROSS_JOIN, leftJoin, rightJoin);

        PlanChecker.from(connectContext, topJoin)
                .checkGroupNum(7)
                .matches(
                        logicalJoin(
                                logicalJoin(
                                        any().when(child -> Objects.equals(child, scanA)),
                                        any().when(child -> Objects.equals(child, scanB))
                                ).when(left -> Objects.equals(left, leftJoin)),

                                logicalJoin(
                                        any().when(child -> Objects.equals(child, scanC)),
                                        any().when(child -> Objects.equals(child, scanD))
                                ).when(right -> Objects.equals(right, rightJoin))
                        ).when(root -> Objects.equals(root, topJoin))
                );
    }

    /*
     * A -> A:
     *
     * unboundRelation(student) -> unboundRelation(student)
     */
    @Test
    public void a2a() {
        UnboundRelation student = new UnboundRelation(ImmutableList.of("student"));
        PlanChecker.from(connectContext, student)
                .applyBottomUp(
                        unboundRelation().then(scan -> scan)
                )
                .checkGroupNum(1)
                .matchesFromRoot(unboundRelation().when(student::equals));
    }

    /*
     * A -> B:
     *
     * unboundRelation(student) -> logicalOlapScan(student)
     */
    @Test
    public void a2b() {
        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);

        PlanChecker.from(connectContext, new UnboundRelation(ImmutableList.of("student")))
                .applyBottomUp(
                        unboundRelation().then(scan -> student)
                )
                .checkGroupNum(1)
                .matchesFromRoot(logicalOlapScan().when(student::equals));
    }

    /*
     * A -> new A:
     *
     * logicalOlapScan(student) -> new logicalOlapScan(student)
     */
    @Test
    public void a2newA() {
        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);

        PlanChecker.from(connectContext, student)
                .applyBottomUp(
                        logicalOlapScan()
                                .when(scan -> student == scan)
                                .then(scan -> new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student))
                )
                .checkGroupNum(1)
                .matchesFromRoot(logicalOlapScan().when(student::equals));
    }

    /*
     * A -> B(C):
     *
     *  unboundRelation(student)               limit(1)
     *                              ->           |
     *                                    logicalOlapScan(student)
     */
    @Test
    public void a2bc() {
        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<LogicalOlapScan> limit = new LogicalLimit<>(1, 0, student);

        PlanChecker.from(connectContext, new UnboundRelation(ImmutableList.of("student")))
                .applyBottomUp(
                        unboundRelation().then(unboundRelation -> limit.withChildren(student))
                )
                .checkGroupNum(2)
                .matchesFromRoot(
                        logicalLimit(
                                logicalOlapScan().when(student::equals)
                        ).when(limit::equals)
                );
    }

    /*
     * A -> B(A): will run into dead loop, so we detect it and throw exception.
     *
     *  unboundRelation(student)               limit(1)                          limit(1)
     *                              ->           |                   ->             |                 ->    ...
     *                                    unboundRelation(student)          unboundRelation(student)
     *
     * you should split A into some states:
     * 1. A(not rewrite)
     * 2. A'(already rewrite)
     *
     * then make sure the A' is new object and not equals to A (overwrite equals method and compare the states),
     * so the case change to 'A -> B(C)', because C has different state with A。
     *
     * the similar case is: A -> B(C(A))
     */
    @Test
    public void a2ba() {
        // invalid case
        Assertions.assertThrows(IllegalStateException.class, () -> {
            UnboundRelation student = new UnboundRelation(ImmutableList.of("student"));
            LogicalLimit<UnboundRelation> limit = new LogicalLimit<>(1, 0, student);

            PlanChecker.from(connectContext, student)
                    .applyBottomUp(
                            unboundRelation().then(unboundRelation -> limit.withChildren(unboundRelation))
                    )
                    .checkGroupNum(2)
                    .matchesFromRoot(
                            logicalLimit(
                                    logicalOlapScan().when(student::equals)
                            ).when(limit::equals)
                    );
        });

        // valid case: 5 steps
        class A extends UnboundRelation {
            // 1: declare the Plan has some states
            final State state;

            public A(List<String> nameParts, State state) {
                this(nameParts, state, Optional.empty());
            }

            public A(List<String> nameParts, State state, Optional<GroupExpression> groupExpression) {
                super(nameParts, groupExpression, Optional.empty());
                this.state = state;
            }

            // 2: overwrite the 'equals' method that compare state
            @Override
            public boolean equals(Object o) {
                return super.equals(o) && state == ((A) o).state;
            }

            // 3: declare 'withState' method, and clear groupExpression(means create new group when rewrite)
            public A withState(State state) {
                return new A(getNameParts(), state, Optional.empty());
            }

            @Override
            public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
                return new A(getNameParts(), state, groupExpression);
            }

            @Override
            public String toString() {
                return "A{namePart=" + getNameParts() + ", state=" + state + '}';
            }
        }

        A a = new A(ImmutableList.of("student"), State.NOT_REWRITE);

        A a2 = new A(ImmutableList.of("student"), State.ALREADY_REWRITE);
        LogicalLimit<UnboundRelation> limit = new LogicalLimit<>(1, 0, a2);

        PlanChecker.from(connectContext, a).applyBottomUp(unboundRelation()
                        // 4: add state condition to the pattern's predicates
                        .when(r -> (r instanceof A) && ((A) r).state == State.NOT_REWRITE).then(unboundRelation -> {
                            // 5: new plan and change state, so this case equal to 'A -> B(C)', which C has
                            //    different state with A
                            A notRewritePlan = (A) unboundRelation;
                            return limit.withChildren(notRewritePlan.withState(State.ALREADY_REWRITE));
                        })).checkGroupNum(2)
                .matchesFromRoot(logicalLimit(unboundRelation().when(a2::equals)).when(limit::equals));
    }

    /*
     * A -> A(B): will run into dead loop, we can not detect it in the group tree, because B usually not equals
     *            to other object (e.g. UnboundRelation), but can detect the rule's invoke times.
     *
     *      limit(1)                             limit(1)                          limit(1)
     *         |                      ->           |                   ->             |                 ->    ...
     * UnboundRelation(student)            UnboundRelation(student)         UnboundRelation(student)
     *
     * you should split A into some states:
     * 1. A(not rewrite)
     * 2. A'(already rewrite)
     *
     * then make sure the A' is new object and not equals to A (overwrite equals method and compare the states),
     * so the case change to 'A -> B(C)', because B has different state with A.
     *
     * the valid example like the 'a2ba' case.
     *
     * the similar case are:
     * 1. A -> A(B(C))
     * 2. A -> B(A(C))
     */
    /*@Test()
    public void a2ab() {
        Assertions.assertThrows(IllegalStateException.class, () -> {
            UnboundRelation student = new UnboundRelation(ImmutableList.of("student"));
            LogicalLimit<UnboundRelation> limit = new LogicalLimit<>(1, 0, student);
            LogicalOlapScan boundStudent = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
            CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, limit);

            PlanChecker.from(cascadesContext)
                    // if this rule invoke times greater than 1000, then throw exception
                    .setMaxInvokeTimesPerRule(1000)
                    .applyBottomUp(
                            logicalLimit().then(l -> l.withChildren(boundStudent))
                    );
        });
    }*/

    /*
     * A -> B(C(D)):
     *
     * unboundRelation(student)   ->     logicalLimit(10)
     *                                         |
     *                                   logicalLimit(5)
     *                                        |
     *                                logicalOlapScan(student)))
     */
    @Test
    public void a2bcd() {
        LogicalOlapScan scan = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<LogicalOlapScan> limit5 = new LogicalLimit<>(5, 0, scan);
        LogicalLimit<LogicalLimit<LogicalOlapScan>> limit10 = new LogicalLimit<>(10, 0, limit5);

        PlanChecker.from(connectContext, limit10)
                .applyBottomUp(
                        unboundRelation().then(r -> limit10)
                )
                .checkGroupNum(3)
                .checkFirstRootLogicalPlan(limit10)
                .matchesFromRoot(
                        logicalLimit(
                                logicalLimit(
                                        logicalOlapScan().when(scan::equals)
                                ).when(limit5::equals)
                        ).when(limit10::equals)
                );
    }

    /*
     * A(B) -> A:
     *
     *       limit(10)                            limit(10)
     *         |                        ->           |
     *  logicalOlapScan(student)              logicalOlapScan(student)
     */
    @Test
    public void ab2a() {
        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<LogicalOlapScan> limit10 = new LogicalLimit<>(10, 0, student);

        PlanChecker.from(connectContext, limit10)
                .applyBottomUp(
                        logicalLimit().when(limit10::equals).then(limit -> limit)
                )
                .checkGroupNum(2)
                .matchesFromRoot(
                        logicalLimit(
                                logicalOlapScan().when(student::equals)
                        ).when(limit10::equals)
                );
    }

    /*
     * A(B) -> new A:
     *
     *       limit(10)                            new limit(10)
     *         |                        ->           |
     *  logicalOlapScan(student)              logicalOlapScan(student)
     */
    @Test
    public void ab2NewA() {
        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<LogicalOlapScan> limit10 = new LogicalLimit<>(10, 0, student);

        PlanChecker.from(connectContext, limit10)
                .applyBottomUp(
                        logicalLimit().when(limit10::equals).then(limit -> limit.withChildren(limit.child()))
                )
                .checkGroupNum(2)
                .matchesFromRoot(
                        logicalLimit(
                                logicalOlapScan().when(student::equals)
                        ).when(limit10::equals)
                );
    }

    /*
     * A(B) -> group B:
     *
     *       limit(10)
     *         |                           ->           group(logicalOlapScan(student))
     *  group(logicalOlapScan(student))
     */
    @Test
    public void ab2GroupB() {
        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<LogicalOlapScan> limit10 = new LogicalLimit<>(10, 0, student);

        PlanChecker.from(connectContext, limit10)
                .applyBottomUp(
                        logicalLimit().when(limit10::equals).then(limit -> limit.child())
                )
                .checkGroupNum(1)
                .matchesFromRoot(
                        logicalOlapScan().when(student::equals)
                );
    }

    /*
     * A(B) -> plan B:
     *
     *       limit(10)
     *         |                        ->           logicalOlapScan(student)
     *  logicalOlapScan(student)
     */
    @Test
    public void ab2PlanB() {
        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<LogicalOlapScan> limit10 = new LogicalLimit<>(10, 0, student);

        PlanChecker.from(connectContext, limit10)
                .applyBottomUp(
                        logicalLimit(logicalOlapScan()).when(limit10::equals).then(limit -> limit.child())
                )
                .checkGroupNum(1)
                .matchesFromRoot(
                        logicalOlapScan().when(student::equals)
                );
    }

    /*
     * A(B) -> C:
     *
     *       limit(10)
     *         |                        ->           logicalOlapScan(student)
     *  unboundRelation(student)
     */
    @Test
    public void ab2c() {
        UnboundRelation relation = new UnboundRelation(ImmutableList.of("student"));
        LogicalLimit<UnboundRelation> limit10 = new LogicalLimit<>(10, 0, relation);

        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        PlanChecker.from(connectContext, limit10)
                .applyBottomUp(
                        logicalLimit(unboundRelation()).then(limit -> student)
                )
                .checkGroupNum(1)
                .matchesFromRoot(
                        logicalOlapScan().when(student::equals)
                );
    }

    /*
     * A(B) -> C(D):
     *
     *       limit(10)                                     limit(5)
     *         |                        ->                    |
     *  unboundRelation(student)                    logicalOlapScan(student)
     */
    @Test
    public void ab2cd() {
        UnboundRelation relation = new UnboundRelation(ImmutableList.of("student"));
        LogicalLimit<UnboundRelation> limit10 = new LogicalLimit<>(10, 0, relation);

        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<LogicalOlapScan> limit5 = new LogicalLimit<>(5, 0, student);

        PlanChecker.from(connectContext, limit10)
                .applyBottomUp(
                        logicalLimit(unboundRelation()).then(limit -> limit5)
                )
                .checkGroupNum(2)
                .matchesFromRoot(
                        logicalLimit(
                                logicalOlapScan().when(student::equals)
                        ).when(limit5::equals)
                );
    }


    /*
     * A(B) -> C(B):
     *
     *       limit(10)                            limit(5)
     *         |                        ->           |
     *  logicalOlapScan(student)              logicalOlapScan(student)
     */
    @Test
    public void ab2cb() {
        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<LogicalOlapScan> limit10 = new LogicalLimit<>(10, 0, student);
        LogicalLimit<LogicalOlapScan> limit5 = new LogicalLimit<>(5, 0, student);

        PlanChecker.from(connectContext, limit10)
                .applyBottomUp(
                        logicalLimit().when(limit10::equals).then(limit -> limit5)
                )
                .checkGroupNum(2)
                .matchesFromRoot(
                        logicalLimit(
                                logicalOlapScan().when(student::equals)
                        ).when(limit5::equals)
                );
    }

    /*
     * A(B) -> new A(new B):
     *
     *       limit(10)                              new limit(10)
     *         |                                          |
     *       limit(5)                               new limit(5)
     *         |                        ->                |
     *  logicalOlapScan(student)              new logicalOlapScan(student)
     *
     * this case is invalid, same as 'a2ab'.
     */
    @Test
    public void ab2NewANewB() {
        Assertions.assertThrowsExactly(IllegalStateException.class, () -> {

            LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
            LogicalLimit<LogicalOlapScan> limit10 = new LogicalLimit<>(10, 0, student);

            PlanChecker.from(connectContext, limit10)
                    .setMaxInvokeTimesPerRule(1000)
                    .applyBottomUp(
                            logicalLimit().when(limit10::equals).then(limit -> limit.withChildren(
                                    new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student)
                            ))
                    );
        });
    }

    /*
     * A(B) -> B(A):
     *
     *     logicalLimit(10)                    logicalLimit(5)
     *          |                                   |
     *     logicalLimit(5)              ->     logicalLimit(10)
     *
     * this case is invalid, we can detect it because this case is similar to 'a2ba', the 'ab2cab' is similar case too
     */
    @Test
    public void ab2ba() {
        Assertions.assertThrowsExactly(IllegalStateException.class, () -> {
            UnboundRelation student = new UnboundRelation(ImmutableList.of("student"));

            LogicalLimit<UnboundRelation> limit5 = new LogicalLimit<>(5, 0, student);
            LogicalLimit<LogicalLimit<UnboundRelation>> limit10 = new LogicalLimit<>(10, 0, limit5);

            PlanChecker.from(connectContext, limit10)
                    .applyBottomUp(
                            logicalLimit(logicalLimit(unboundRelation())).when(limit10::equals).then(l ->
                                    l.child().withChildren(
                                            l
                                    )
                            )
                    );
        });
    }

    /*
     * A(B) -> C(D(E)):
     *
     * logicalLimit(3)            ->         logicalLimit(10)
     *       |                                    |
     * unboundRelation(student)              logicalLimit(5)
     *                                           |
     *                                   logicalOlapScan(student)))
     */
    @Test
    public void ab2cde() {
        UnboundRelation student = new UnboundRelation(ImmutableList.of("student"));
        LogicalLimit<UnboundRelation> limit3 = new LogicalLimit<>(3, 0, student);

        LogicalOlapScan scan = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<LogicalOlapScan> limit5 = new LogicalLimit<>(5, 0, scan);
        LogicalLimit<LogicalLimit<LogicalOlapScan>> limit10 = new LogicalLimit<>(10, 0, limit5);

        PlanChecker.from(connectContext, limit3)
                .applyBottomUp(
                        logicalLimit(unboundRelation()).then(l -> limit10)
                )
                .checkGroupNum(3)
                .matchesFromRoot(
                        logicalLimit(
                                logicalLimit(
                                        logicalOlapScan().when(scan::equals)
                                ).when(limit5::equals)
                        ).when(limit10::equals)
                );
    }

    /*
     * A(B(C)) -> B(A(C)):
     *
     *     logicalLimit(10)                    logicalLimit(5)
     *          |                                   |
     *     logicalLimit(5)              ->     logicalLimit(10)
     *         |                                   |
     * logicalOlapScan(student)))          logicalOlapScan(student)))
     */
    @Test
    public void abc2bac() {
        UnboundRelation student = new UnboundRelation(ImmutableList.of("student"));

        LogicalLimit<UnboundRelation> limit5 = new LogicalLimit<>(5, 0, student);
        LogicalLimit<LogicalLimit<UnboundRelation>> limit10 = new LogicalLimit<>(10, 0, limit5);

        PlanChecker.from(connectContext, limit10)
                .applyBottomUp(
                        logicalLimit(logicalLimit(unboundRelation())).when(limit10::equals).then(l ->
                                // limit 5
                                l.child().withChildren(
                                        // limit 10
                                        l.withChildren(
                                                // student
                                                l.child().child()
                                        )
                                )
                        )
                )
                .checkGroupNum(3)
                .matchesFromRoot(
                        logicalLimit(
                                logicalLimit(
                                        unboundRelation().when(student::equals)
                                ).when(limit10::equals)
                        ).when(limit5::equals)
                );
    }

    /*
     * A(B(C)) -> A(C):
     *
     *     logicalLimit(10)                        logicalLimit(10)
     *          |                                        |
     *     logicalLimit(5)              ->     logicalOlapScan(student)))
     *         |
     * logicalOlapScan(student)))
     */
    @Test
    public void abc2bc() {
        UnboundRelation student = new UnboundRelation(ImmutableList.of("student"));

        LogicalLimit<UnboundRelation> limit5 = new LogicalLimit<>(5, 0, student);
        LogicalLimit<LogicalLimit<UnboundRelation>> limit10 = new LogicalLimit<>(10, 0, limit5);

        PlanChecker.from(connectContext, limit10)
                .applyBottomUp(
                        logicalLimit(logicalLimit(unboundRelation())).then(l ->
                                // limit 10
                                l.withChildren(
                                        // student
                                        l.child().child()
                                )
                        )
                )
                .checkGroupNum(2)
                .matchesFromRoot(
                        logicalLimit(
                                unboundRelation().when(student::equals)
                        ).when(limit10::equals)
                );
    }

    @Test
    public void testRewriteBottomPlanToOnePlan() {
        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<LogicalOlapScan> limit = new LogicalLimit<>(1, 0, student);

        LogicalOlapScan score = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.score);

        PlanChecker.from(connectContext, limit)
                .applyBottomUp(
                        logicalOlapScan().when(scan -> Objects.equals(student, scan)).then(scan -> score)
                )
                .checkGroupNum(2)
                .matchesFromRoot(
                        logicalLimit(
                                logicalOlapScan().when(score::equals)
                        ).when(limit::equals)
                );
    }

    @Test
    public void testRewriteBottomPlanToMultiPlan() {
        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<LogicalOlapScan> limit10 = new LogicalLimit<>(10, 0, student);

        LogicalOlapScan score = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.score);
        LogicalLimit<LogicalOlapScan> limit1 = new LogicalLimit<>(1, 0, score);

        PlanChecker.from(connectContext, limit10)
                .applyBottomUp(
                        logicalOlapScan().when(scan -> Objects.equals(student, scan)).then(scan -> limit1)
                )
                .checkGroupNum(3)
                .matchesFromRoot(
                        logicalLimit(
                                logicalLimit(
                                        any().when(score::equals)
                                ).when(limit1::equals)
                        ).when(limit10::equals)
                );
    }

    @Test
    public void testRewriteUnboundPlanToBound() {
        UnboundRelation unboundTable = new UnboundRelation(ImmutableList.of("score"));
        LogicalOlapScan boundTable = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.score);

        PlanChecker.from(connectContext, unboundTable)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertTrue(logicalProperties instanceof UnboundLogicalProperties);
                })
                .applyBottomUp(unboundRelation().then(unboundRelation -> boundTable))
                .checkGroupNum(1)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertEquals(
                            boundTable.getLogicalProperties().getOutput(), logicalProperties.getOutput());
                })
                .matchesFromRoot(
                        logicalOlapScan().when(boundTable::equals)
                );
    }

    @Test
    public void testRecomputeLogicalProperties() {
        UnboundRelation unboundTable = new UnboundRelation(ImmutableList.of("score"));
        LogicalLimit<UnboundRelation> unboundLimit = new LogicalLimit<>(1, 0, unboundTable);

        LogicalOlapScan boundTable = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.score);
        LogicalLimit<Plan> boundLimit = unboundLimit.withChildren(ImmutableList.of(boundTable));

        PlanChecker.from(connectContext, unboundLimit)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertTrue(logicalProperties instanceof UnboundLogicalProperties);
                })
                .applyBottomUp(unboundRelation().then(unboundRelation -> boundTable))
                .applyBottomUp(
                        logicalPlan()
                                .when(plan -> plan.canBind() && !(plan instanceof LeafPlan))
                                .then(LogicalPlan::recomputeLogicalProperties)
                )
                .checkGroupNum(2)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertEquals(
                            boundTable.getLogicalProperties().getOutput(), logicalProperties.getOutput());
                })
                .matchesFromRoot(
                        logicalLimit(
                                logicalOlapScan().when(boundTable::equals)
                        ).when(boundLimit::equals)
                );
    }

    @Test
    public void testEliminateRootWithChildGroupInTwoLevels() {
        LogicalOlapScan scan = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.score);
        LogicalLimit<Plan> limit = new LogicalLimit<>(1, 0, scan);

        PlanChecker.from(connectContext, limit)
                .applyBottomUp(logicalLimit().then(LogicalLimit::child))
                .checkGroupNum(1)
                .checkGroupExpressionNum(1)
                .checkFirstRootLogicalPlan(scan);
    }

    @Test
    public void testEliminateRootWithChildPlanInTwoLevels() {
        LogicalOlapScan scan = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.score);
        LogicalLimit<Plan> limit = new LogicalLimit<>(1, 0, scan);

        PlanChecker.from(connectContext, limit)
                .applyBottomUp(logicalLimit(any()).then(LogicalLimit::child))
                .checkGroupNum(1)
                .checkGroupExpressionNum(1)
                .checkFirstRootLogicalPlan(scan);
    }

    @Test
    public void testEliminateTwoLevelsToOnePlan() {
        LogicalOlapScan score = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.score);
        LogicalLimit<Plan> limit = new LogicalLimit<>(1, 0, score);

        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);

        PlanChecker.from(connectContext, limit)
                .applyBottomUp(logicalLimit(any()).then(l -> student))
                .checkGroupNum(1)
                .checkGroupExpressionNum(1)
                .checkFirstRootLogicalPlan(student);

        PlanChecker.from(connectContext, limit)
                .applyBottomUp(logicalLimit(group()).then(l -> student))
                .checkGroupNum(1)
                .checkGroupExpressionNum(1)
                .checkFirstRootLogicalPlan(student);
    }

    @Test
    public void testEliminateTwoLevelsToTwoPlans() {
        LogicalOlapScan score = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.score);
        LogicalLimit<Plan> limit1 = new LogicalLimit<>(1, 0, score);

        LogicalOlapScan student = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student);
        LogicalLimit<Plan> limit10 = new LogicalLimit<>(10, 0, student);

        PlanChecker.from(connectContext, limit1)
                .applyBottomUp(logicalLimit(any()).when(limit1::equals).then(l -> limit10))
                .checkGroupNum(2)
                .checkGroupExpressionNum(2)
                .matchesFromRoot(
                        logicalLimit(
                                logicalOlapScan().when(student::equals)
                        ).when(limit10::equals)
                );

        PlanChecker.from(connectContext, limit1)
                .applyBottomUp(logicalLimit(group()).when(limit1::equals).then(l -> limit10))
                .checkGroupNum(2)
                .checkGroupExpressionNum(2)
                .matchesFromRoot(
                        logicalLimit(
                                logicalOlapScan().when(student::equals)
                        ).when(limit10::equals)
                );
    }

    @Test
    public void test() {
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze(new LogicalLimit<>(10, 0,
                        new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                                ImmutableList.of(new EqualTo(new UnboundSlot("sid"), new UnboundSlot("id"))),
                                new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.score),
                                new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student)
                        )
                ))
                .applyTopDown(
                        logicalLimit(logicalJoin()).then(limit -> {
                            LogicalJoin<GroupPlan, GroupPlan> join = limit.child();
                            switch (join.getJoinType()) {
                                case LEFT_OUTER_JOIN:
                                    return join.withChildren(limit.withChildren(join.left()), join.right());
                                case RIGHT_OUTER_JOIN:
                                    return join.withChildren(join.left(), limit.withChildren(join.right()));
                                case CROSS_JOIN:
                                    return join.withChildren(limit.withChildren(join.left()), limit.withChildren(join.right()));
                                case INNER_JOIN:
                                    if (!join.getHashJoinConjuncts().isEmpty()) {
                                        return join.withChildren(
                                                limit.withChildren(join.left()),
                                                limit.withChildren(join.right())
                                        );
                                    } else {
                                        return limit;
                                    }
                                case LEFT_ANTI_JOIN:
                                    // todo: support anti join.
                                default:
                                    return limit;
                            }
                        })
                )
                .matchesFromRoot(
                        logicalJoin(
                                logicalLimit(
                                        logicalOlapScan()
                                ),
                                logicalOlapScan()
                        )
                );
    }


    /**
     * Original:
     * Project(name)
     * |---Project(name)
     *     |---UnboundRelation
     *
     * After rewrite:
     * Project(name)
     * |---Project(rewrite)
     *     |---Project(rewrite_inside)
     *         |---UnboundRelation
     */
    @Test
    public void testRewriteMiddlePlans() {
        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        LogicalProject insideProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                unboundRelation
        );
        LogicalProject rootProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                insideProject
        );

        // Project -> Project -> Relation
        Memo memo = new Memo(rootProject);
        Group leafGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 0).findFirst().get();
        Group targetGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 1).findFirst().get();
        LogicalProject rewriteInsideProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("rewrite_inside", StringType.INSTANCE,
                        false, ImmutableList.of("test"))),
                new GroupPlan(leafGroup)
        );
        LogicalProject rewriteProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("rewrite", StringType.INSTANCE,
                        true, ImmutableList.of("test"))),
                rewriteInsideProject
        );
        memo.copyIn(rewriteProject, targetGroup, true);

        Assertions.assertEquals(4, memo.getGroups().size());
        Plan node = memo.copyOut();
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals("name", ((LogicalProject<?>) node).getProjects().get(0).getName());
        node = node.child(0);
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals("rewrite", ((LogicalProject<?>) node).getProjects().get(0).getName());
        node = node.child(0);
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals("rewrite_inside", ((LogicalProject<?>) node).getProjects().get(0).getName());
        node = node.child(0);
        Assertions.assertTrue(node instanceof UnboundRelation);
        Assertions.assertEquals("test", ((UnboundRelation) node).getTableName());
    }

    /**
     * Test rewrite current Plan with its child.
     *
     * Original(Group 2 is root):
     * Group2: Project(outside)
     * Group1: |---Project(inside)
     * Group0:     |---UnboundRelation
     *
     * and we want to rewrite group 2 by Project(inside, GroupPlan(group 0))
     *
     * After rewriting we should get(Group 2 is root):
     * Group2: Project(inside)
     * Group0: |---UnboundRelation
     */
    @Test
    public void testEliminateRootWithChildPlanThreeLevels() {
        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        LogicalProject<UnboundRelation> insideProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("inside", StringType.INSTANCE, true, ImmutableList.of("test"))),
                unboundRelation
        );
        LogicalProject<LogicalProject<UnboundRelation>> rootProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("outside", StringType.INSTANCE, true, ImmutableList.of("test"))),
                insideProject
        );

        // Project -> Project -> Relation
        Memo memo = new Memo(rootProject);
        Group leafGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 0).findFirst().get();
        Group targetGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 2).findFirst().get();
        LogicalPlan rewriteProject = insideProject.withChildren(Lists.newArrayList(new GroupPlan(leafGroup)));
        memo.copyIn(rewriteProject, targetGroup, true);

        Assertions.assertEquals(2, memo.getGroups().size());
        Plan node = memo.copyOut();
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals(insideProject.getProjects().get(0), ((LogicalProject<?>) node).getProjects().get(0));
        node = node.child(0);
        Assertions.assertTrue(node instanceof UnboundRelation);
        Assertions.assertEquals("test", ((UnboundRelation) node).getTableName());

        // check Group 1's GroupExpression is not in GroupExpressionMaps anymore
        GroupExpression groupExpression = new GroupExpression(rewriteProject, Lists.newArrayList(leafGroup));
        Assertions.assertEquals(2,
                memo.getGroupExpressions().get(groupExpression).getOwnerGroup().getGroupId().asInt());
    }

    private enum State {
        NOT_REWRITE, ALREADY_REWRITE
    }
}
