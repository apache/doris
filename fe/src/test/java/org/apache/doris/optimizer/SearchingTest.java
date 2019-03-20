package org.apache.doris.optimizer;

import com.google.common.collect.Lists;
import org.apache.doris.optimizer.base.SearchVariable;
import org.apache.doris.optimizer.rule.transformation.OptUTInternalAssociativityRule;
import org.apache.doris.optimizer.rule.transformation.OptUTInternalCommutativityRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SearchingTest {
    private static final Logger LOG = LogManager.getLogger(OptMemo.class);

    @Test
    public void testSingleJoin() {
        final OptExpression expr = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final Optimizer optimizer = new Optimizer(expr, variable);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        Assert.assertTrue(checkStatusFinishOptimization(optimizer.getMemo().getGroups()));
        Assert.assertEquals(3, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(4, optimizer.getMemo().getMExprs().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(0).getMultiExpressions().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(1).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(2).getMultiExpressions().size());
        Assert.assertEquals(2, getPlanCount(optimizer.getRoot()));

    }

    @Test
    public void testTwoJoin() {
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        final OptExpression topJoin = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf());
        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final Optimizer optimizer = new Optimizer(topJoin, variable);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        Assert.assertTrue(checkStatusFinishOptimization(optimizer.getMemo().getGroups()));
        Assert.assertEquals(7, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(15, optimizer.getMemo().getMExprs().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(0).getMultiExpressions().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(1).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(2).getMultiExpressions().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(3).getMultiExpressions().size());
        Assert.assertEquals(6, optimizer.getMemo().getGroups().get(4).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(5).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(6).getMultiExpressions().size());
        Assert.assertEquals(12, getPlanCount(optimizer.getRoot()));
    }

    @Test
    public void testThreeJoin() {
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        final OptExpression bottom2Join = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf());
        final OptExpression topJoin = Utils.createUtInternal(
                bottom2Join,
                Utils.createUtLeaf());
        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final Optimizer optimizer = new Optimizer(topJoin, variable);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        Assert.assertTrue(checkStatusFinishOptimization(optimizer.getMemo().getGroups()));
        Assert.assertEquals(15, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(54, optimizer.getMemo().getMExprs().size());
        Assert.assertEquals(120, getPlanCount(optimizer.getRoot()));
    }

    @Test
    public void testFourJoin() {
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        final OptExpression bottom2Join = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf());
        final OptExpression bottom3Join = Utils.createUtInternal(
                bottom2Join,
                Utils.createUtLeaf());
        final OptExpression topJoin = Utils.createUtInternal(
                bottom3Join,
                Utils.createUtLeaf());
        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final Optimizer optimizer = new Optimizer(topJoin, variable);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();
        LOG.info("result:\n" + optimizer.getRoot().getExplain());
//        Assert.assertTrue(checkStatusFinishOptimization(optimizer.getMemo().getGroups()));
//        Assert.assertEquals(31, optimizer.getMemo().getGroups().size());
//        Assert.assertEquals(185, optimizer.getMemo().getMExprs().size());
//        Assert.assertEquals(1680, getPlanCount(optimizer.getRoot()));
    }

//    @Test
//    public void testTenJoin() {
//        final OptExpression bottomJoin = Utils.createUtInternal(
//                Utils.createUtLeaf(),
//                Utils.createUtLeaf());
//        final OptExpression bottom2Join = Utils.createUtInternal(
//                bottomJoin,
//                Utils.createUtLeaf());
//        final OptExpression bottom3Join = Utils.createUtInternal(
//                bottom2Join,
//                Utils.createUtLeaf());
//        final OptExpression bottom4Join = Utils.createUtInternal(
//                bottom3Join,
//                Utils.createUtLeaf());
//        final OptExpression bottom5Join = Utils.createUtInternal(
//                bottom4Join,
//                Utils.createUtLeaf());
//        final OptExpression bottom6Join = Utils.createUtInternal(
//                bottom5Join,
//                Utils.createUtLeaf());
//        final OptExpression bottom7Join = Utils.createUtInternal(
//                bottom6Join,
//                Utils.createUtLeaf());
//        final OptExpression bottom8Join = Utils.createUtInternal(
//                bottom7Join,
//                Utils.createUtLeaf());
//        final OptExpression bottom9Join = Utils.createUtInternal(
//                bottom8Join,
//                Utils.createUtLeaf());
//        final OptExpression topJoin = Utils.createUtInternal(
//                bottom9Join,
//                Utils.createUtLeaf());
//
//        LOG.info("expr=\n{}", topJoin.getExplainString());
//        final Optimizer optimizer = new Optimizer(topJoin);
//        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
//        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
//        optimizer.optimize();
//        final long possibleSolutions = getPossibleSolutions(optimizer.getRoot());
//        LOG.info("possibleSolutions:" + possibleSolutions);
//        Assert.assertTrue(checkStatusFinishOptimization(optimizer.getMemo().getGroups()));
//        Assert.assertEquals(1680, possibleSolutions);
//    }

    private boolean checkStatusFinishOptimization(List<OptGroup> groups) {
        final List<OptGroup> unoptimizedGroups = Lists.newArrayList();
        final List<MultiExpression> unimplementedMExprs = Lists.newArrayList();
        for (OptGroup group : groups) {
            if (!group.isOptimized()) {
                unoptimizedGroups.add(group);
            }
            for (MultiExpression mExpr : group.getMultiExpressions()) {
                if (!mExpr.isImplemented()) {
                    unimplementedMExprs.add(mExpr);
                }
            }
        }

        final StringBuilder unoptimizedGroupsLog = new StringBuilder("Unoptimized Group ids:");
        for (OptGroup group : unoptimizedGroups) {
            unoptimizedGroupsLog.append(group.getId()).append(" status:")
                    .append(group.getStatus()).append(" MultiExpression ids:");
            for (MultiExpression mExpr : group.getMultiExpressions()) {
                unoptimizedGroupsLog.append(mExpr.getId()).append(" status:").append(mExpr.getStatus()).append(", ");
            }
        }
        LOG.info(unoptimizedGroupsLog);

        final StringBuilder unimplementedMExprLog = new StringBuilder("Unimplemented MultiExpression ids:");
        for (MultiExpression mExpr : unimplementedMExprs) {
            unimplementedMExprLog.append(mExpr.getOp()).append(" ").append(mExpr.getId()).append(", ");
        }
        LOG.info(unimplementedMExprLog);

        return unoptimizedGroups.size() == 0 && unimplementedMExprs.size() == 0 ? true : false;
    }


    private long getPlanCount(OptGroup group) {
        long possibleSolutionsTotal = 0;
        for (MultiExpression mExpr : group.getMultiExpressions()) {
            long possibleSolutions = 1;
            for (OptGroup child : mExpr.getInputs()) {
                possibleSolutions *= getPlanCount(child);
            }
            possibleSolutionsTotal += possibleSolutions;
        }
        return possibleSolutionsTotal;
    }
}
