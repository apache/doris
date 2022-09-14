package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SemiJoinLogicalJoinTransposeProjectTest {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private static final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testSemiJoinLogicalTransposeProjectLAsscom() {
        /*-
         *     topSemiJoin                    project
         *      /     \                         |
         *   project   C                    newTopJoin
         *      |            ->            /         \
         *  bottomJoin            newBottomSemiJoin   B
         *   /    \                    /      \
         *  A      B                  A        C
         */
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))  // t1.id = t2.id
                .project(ImmutableList.of(0))
                .hashJoinUsing(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))  // t1.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .transform(SemiJoinLogicalJoinTransposeProject.LEFT_DEEP.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(2, root.getLogicalExpressions().size());
                    Plan plan = memo.copyOut(root.getLogicalExpressions().get(1), false);

                    LogicalJoin<?, ?> newTopJoin = (LogicalJoin<?, ?>) plan.child(0);
                    LogicalJoin<?, ?> newBottomJoin = (LogicalJoin<?, ?>) newTopJoin.left();
                    Assertions.assertEquals(JoinType.INNER_JOIN, newTopJoin.getJoinType());
                    Assertions.assertEquals(JoinType.LEFT_SEMI_JOIN, newBottomJoin.getJoinType());

                    LogicalOlapScan newBottomJoinLeft = (LogicalOlapScan) newBottomJoin.left();
                    LogicalOlapScan newBottomJoinRight = (LogicalOlapScan) newBottomJoin.right();
                    LogicalOlapScan newTopJoinRight = (LogicalOlapScan) newTopJoin.right();

                    Assertions.assertEquals("t1", newBottomJoinLeft.getTable().getName());
                    Assertions.assertEquals("t3", newBottomJoinRight.getTable().getName());
                    Assertions.assertEquals("t2", newTopJoinRight.getTable().getName());
                });
    }

    @Test
    public void testSemiJoinLogicalTransposeProjectLAsscomFail() {
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .project(ImmutableList.of(0, 2)) // t1.id, t2.id
                .hashJoinUsing(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(1, 0)) // t2.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .transform(SemiJoinLogicalJoinTransposeProject.LEFT_DEEP.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(1, root.getLogicalExpressions().size());
                });
    }

    @Test
    public void testSemiJoinLogicalTransposeProjectAll() {
        /*-
         *     topSemiJoin                  project
         *       /     \                       |
         *    project   C                  newTopJoin
         *       |                        /         \
         *  bottomJoin  C     -->       A     newBottomSemiJoin
         *   /    \                               /      \
         *  A      B                             B       C
         */
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .project(ImmutableList.of(0, 2)) // t1.id, t2.id
                .hashJoinUsing(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(1, 0)) // t2.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .transform(SemiJoinLogicalJoinTransposeProject.ALL.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(2, root.getLogicalExpressions().size());
                    Plan plan = memo.copyOut(root.getLogicalExpressions().get(1), false);

                    LogicalJoin<?, ?> newTopJoin = (LogicalJoin<?, ?>) plan.child(0);
                    LogicalJoin<?, ?> newBottomJoin = (LogicalJoin<?, ?>) newTopJoin.right();
                    Assertions.assertEquals(JoinType.INNER_JOIN, newTopJoin.getJoinType());
                    Assertions.assertEquals(JoinType.LEFT_SEMI_JOIN, newBottomJoin.getJoinType());

                    LogicalOlapScan newBottomJoinLeft = (LogicalOlapScan) newBottomJoin.left();
                    LogicalOlapScan newBottomJoinRight = (LogicalOlapScan) newBottomJoin.right();
                    LogicalOlapScan newTopJoinLeft = (LogicalOlapScan) newTopJoin.left();

                    Assertions.assertEquals("t1", newTopJoinLeft.getTable().getName());
                    Assertions.assertEquals("t2", newBottomJoinLeft.getTable().getName());
                    Assertions.assertEquals("t3", newBottomJoinRight.getTable().getName());
                });
    }
}
